# ============================================================
# ukg_pipeline.py — Orchestrator: Load Staging → MERGE → Audit
# UKG → Oracle ETL — Phase 1 (SQL Staging approach)
# ============================================================
import logging
import os
import sys
import uuid

import pyodbc

from ukg_loader import load_csv_to_staging

DB_CONN = os.getenv(
    "UKG_DB_CONN",
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;DATABASE=HR_OATRH;UID=sa;PWD=YourStrongPassw0rd;"
    "Encrypt=no;TrustServerCertificate=yes;",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ukg-pipeline")


def run_pipeline(csv_path: str) -> dict:
    """Run full pipeline: load staging → MERGE → audit. Idempotent."""

    # ─── Step 1: Load CSV → staging ─────────────────────────
    logger.info("Step 1/3: Loading CSV → staging table…")
    result = load_csv_to_staging(csv_path)
    batch_id = result["batch_id"]

    if result["total_rows"] == 0:
        logger.warning("No rows loaded. Aborting.")
        return {**result, "merge_status": "SKIPPED"}

    # ─── Step 2: Execute MERGE ──────────────────────────────
    logger.info("Step 2/3: Executing MERGE (atomic)…")
    conn = pyodbc.connect(DB_CONN, autocommit=True)
    cursor = conn.cursor()

    try:
        cursor.execute("EXEC dbo.ukg_merge_employees @batch_id=?", batch_id)
        merge_result = cursor.fetchone()
        logger.info(
            "  MERGE complete — %s rows affected, hash=%s",
            merge_result[2] if merge_result else "?",
            (merge_result[3] or "N/A")[:16] if merge_result else "N/A",
        )
    except Exception as exc:
        logger.exception("MERGE failed: %s", exc)
        return {**result, "merge_status": "FAILED", "error": str(exc)}
    finally:
        conn.close()

    # ─── Step 3: Verify ─────────────────────────────────────
    logger.info("Step 3/3: Verification…")
    conn = pyodbc.connect(DB_CONN, autocommit=True)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT status, total_rows, inserted_rows, error_rows, import_hash "
        "FROM dbo.ukg_import_log WHERE batch_id=?",
        batch_id,
    )
    log = cursor.fetchone()
    conn.close()

    summary = {
        "batch_id": batch_id,
        "filename": result["filename"],
        "status": log[0] if log else "UNKNOWN",
        "total_rows": result["total_rows"],
        "rows_affected": log[2] if log else 0,
        "errors": result["error_rows"],
        "audit_hash": (log[4] or "")[:16] + "…" if log and log[4] else "N/A",
    }

    print()
    print("=" * 55)
    print("  UKG → Oracle ETL — PIPELINE COMPLETE")
    print("=" * 55)
    for k, v in summary.items():
        print(f"  {k:<18} {v}")
    print("=" * 55)

    return summary


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Uso: python {sys.argv[0]} <csv_path>")
        sys.exit(1)

    csv_path = sys.argv[1]
    run_pipeline(csv_path)
