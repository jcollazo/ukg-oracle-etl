# ============================================================
# crypto_utils.py — AES-256-GCM encryption for sensitive fields
# UKG → Oracle ETL — Phase 1
# ============================================================
# SSN encryption at application layer BEFORE storage.
# Ciphertext format: base64(nonce[12] || ciphertext || tag[16])
# Decryption only at app layer when data needs to be read.
# ============================================================
import base64
import logging
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logger = logging.getLogger("ukg-crypto")

# ─── Key Management ──────────────────────────────────────────
# KEY: 32 bytes (AES-256), base64-encoded in env var UKG_ENCRYPTION_KEY
# Generate: python -c "import os,base64; print(base64.b64encode(os.urandom(32)).decode())"
# Store: NEVER commit to source control. Set in env or secrets manager.

_ENCRYPTION_KEY: bytes | None = None


def _get_key() -> bytes:
    """Lazy-load encryption key from environment."""
    global _ENCRYPTION_KEY
    if _ENCRYPTION_KEY is not None:
        return _ENCRYPTION_KEY

    key_b64 = os.getenv("UKG_ENCRYPTION_KEY")
    if not key_b64:
        raise RuntimeError(
            "UKG_ENCRYPTION_KEY not set. "
            "Generate: python -c \"import os,base64; print(base64.b64encode(os.urandom(32)).decode())\""
        )

    _ENCRYPTION_KEY = base64.b64decode(key_b64)
    if len(_ENCRYPTION_KEY) != 32:
        raise ValueError(f"UKG_ENCRYPTION_KEY must decode to 32 bytes (got {len(_ENCRYPTION_KEY)})")

    return _ENCRYPTION_KEY


def encrypt(value: str | None) -> str | None:
    """Encrypt a string with AES-256-GCM. Returns base64 ciphertext.

    Args:
        value: Plaintext string to encrypt, or None (pass-through).

    Returns:
        Base64-encoded ciphertext (nonce + ciphertext + tag), or None.

    >>> cipher = encrypt("583-12-3456")
    >>> len(cipher) > 11  # Ciphertext always longer than plaintext
    True
    >>> encrypt(None) is None
    True
    """
    if value is None:
        return None

    key = _get_key()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit nonce, random per encryption

    ciphertext = aesgcm.encrypt(nonce, value.encode("utf-8"), None)
    # Format: nonce[12] + ciphertext (includes 16-byte tag automatically)
    return base64.b64encode(nonce + ciphertext).decode("ascii")


def decrypt(value: str | None) -> str | None:
    """Decrypt AES-256-GCM ciphertext back to plaintext.

    Args:
        value: Base64-encoded ciphertext, or None (pass-through).

    Returns:
        Decrypted plaintext string, or None.

    >>> key_b64 = base64.b64encode(os.urandom(32)).decode()
    >>> os.environ["UKG_ENCRYPTION_KEY"] = key_b64
    >>> cipher = encrypt("583-12-3456")
    >>> decrypt(cipher)
    '583-12-3456'
    >>> decrypt(None) is None
    True
    """
    if value is None:
        return None

    key = _get_key()
    aesgcm = AESGCM(key)

    raw = base64.b64decode(value)
    nonce = raw[:12]
    ciphertext = raw[12:]

    return aesgcm.decrypt(nonce, ciphertext, None).decode("utf-8")


def mask_ssn(value: str | None) -> str | None:
    """Mask SSN for logging: 'XXX-XX-1234'.

    Accepts raw SSN (before encryption) or None.

    Args:
        value: Raw SSN string or None.

    Returns:
        Masked SSN like 'XXX-XX-1234' or None.
    """
    if not value or not value.strip():
        return None

    ssn = value.strip()
    digits = ssn.replace("-", "")

    if len(digits) == 9 and digits.isdigit():
        return f"XXX-XX-{digits[-4:]}"

    logger.warning("Invalid SSN format for masking: %s...", ssn[:3])
    return None


def rotate_key(new_key_b64: str, conn) -> int:
    """Re-encrypt all SSNs in staging and target tables with a new key.

    WARNING: This is a heavy operation. Run during maintenance window.
    Requires both OLD and NEW keys to be available.

    Args:
        new_key_b64: New base64-encoded 32-byte key.
        conn: Active pyodbc connection.

    Returns:
        Number of rows re-encrypted.
    """
    global _ENCRYPTION_KEY
    old_key = _get_key()
    new_key = base64.b64decode(new_key_b64)
    if len(new_key) != 32:
        raise ValueError(f"New key must decode to 32 bytes (got {len(new_key)})")

    # Temporarily use old key for decryption
    old_aesgcm = AESGCM(old_key)
    new_aesgcm = AESGCM(new_key)
    total = 0

    cursor = conn.cursor()

    # Rotate staging table
    cursor.execute("SELECT id, ssn FROM dbo.ukg_staging WHERE ssn IS NOT NULL")
    for row_id, ciphertext in cursor.fetchall():
        if not ciphertext:
            continue
        try:
            raw = base64.b64decode(ciphertext)
            nonce = raw[:12]
            ct = raw[12:]
            plaintext = old_aesgcm.decrypt(nonce, ct, None)

            new_nonce = os.urandom(12)
            new_ct = new_aesgcm.encrypt(new_nonce, plaintext, None)
            new_cipher = base64.b64encode(new_nonce + new_ct).decode("ascii")

            cursor.execute(
                "UPDATE dbo.ukg_staging SET ssn=? WHERE id=?", new_cipher, row_id
            )
            total += 1
        except Exception as exc:
            logger.error("Failed to rotate SSN id=%d: %s", row_id, exc)

    # Rotate empleados table
    cursor.execute("SELECT id, ssn FROM dbo.empleados WHERE ssn IS NOT NULL")
    for row_id, ciphertext in cursor.fetchall():
        if not ciphertext:
            continue
        try:
            raw = base64.b64decode(ciphertext)
            nonce = raw[:12]
            ct = raw[12:]
            plaintext = old_aesgcm.decrypt(nonce, ct, None)

            new_nonce = os.urandom(12)
            new_ct = new_aesgcm.encrypt(new_nonce, plaintext, None)
            new_cipher = base64.b64encode(new_nonce + new_ct).decode("ascii")

            cursor.execute(
                "UPDATE dbo.empleados SET ssn=? WHERE id=?", new_cipher, row_id
            )
            total += 1
        except Exception as exc:
            logger.error("Failed to rotate SSN id=%d: %s", row_id, exc)

    conn.commit()

    # Swap keys
    _ENCRYPTION_KEY = new_key

    logger.info("Key rotation complete: %d rows re-encrypted", total)
    return total
