# UKG → Oracle ETL

**Pipeline de integración UKG Pro → base de datos SQL Server.**
Cumplimiento Ley 126-2012 — hash chain criptográfico por lote.

Phase 1: SQL Staging approach — CSV → staging table → MERGE atómico → audit hash chain.

---

## 🏗️ Arquitectura

```
┌──────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────┐
│ SFTP     │─────▶│ ukg_pipeline │─────▶│ dbo.ukg_     │─────▶│ dbo.     │
│ incoming │      │ .py          │      │ staging      │      │ target   │
└──────────┘      └──────────────┘      └──────┬───────┘      └──────────┘
                                               │
                                        ┌──────▼───────┐
                                        │ MERGE proc   │ ◀── Atómico
                                        │ (idempotent) │     SQL Server
                                        └──────┬───────┘
                                               │
                                        ┌──────▼───────┐
                                        │ Hash SHA-256 │ ◀── Ley 126-2012
                                        │ audit chain  │
                                        └──────────────┘
```

| Componente | Rol | RAM |
|---|---|---|
| `ukg_loader.py` | CSV → staging table (streaming) | ~10 MB |
| `02_merge_procedure.sql` | MERGE staging → destino (SQL Server) | 0 MB |
| `ukg_pipeline.py` | Orchestrator: load → merge → audit | ~15 MB |

---

## 🚀 Quick Start

### 1. Configurar base de datos

```bash
# Set your target database
export UKG_DB_CONN="DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=YourDB;UID=sa;PWD=YourPassword;Encrypt=no;TrustServerCertificate=yes;"
```

### 2. Crear tablas en SQL Server

```bash
sqlcmd -S localhost -U sa -P "YourPassword" -d YourDB \
  -i sql/01_staging_tables.sql \
  -i sql/02_merge_procedure.sql
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Pipeline completo

```bash
# One-shot
python src/ukg_pipeline.py /sftp/employees_2026-06-22.csv

# Solo staging (sin MERGE)
python src/ukg_loader.py /sftp/employees_2026-06-22.csv
```

### 5. Ejecutar MERGE manual

```sql
EXEC dbo.ukg_merge_employees @batch_id = 'XXXXXXXX-XXXX-...'
```

---

## ⏰ Cron (diario 2 AM)

```bash
# crontab -e
0 2 * * * python /opt/ukg-oracle-etl/src/ukg_pipeline.py /sftp/incoming/latest.csv >> /opt/ukg-oracle-etl/logs/import.log 2>&1
```

---

## 🛡️ Resiliencia

| Feature | Implementación |
|---|---|
| **Resume tras crash** | `dbo.ukg_checkpoint` guarda última fila procesada |
| **Dead letter queue** | `dbo.ukg_error_log` captura filas con error |
| **Idempotente** | MERGE usa key compuesta como identificador único |
| **Atómico** | MERGE en una sola transacción SQL |
| **Rollback** | `SET XACT_ABORT ON` — error → rollback completo |
| **Audit** | SHA-256 hash chain por batch — Ley 126-2012 |

---

## 📊 Monitoreo

```sql
-- Últimos 10 imports
SELECT TOP 10 batch_id, filename, status, total_rows, inserted_rows, error_rows, completed_at
FROM dbo.ukg_import_log
ORDER BY created_at DESC;

-- Errores del último batch
SELECT row_number, eeid, error_message
FROM dbo.ukg_error_log
WHERE batch_id = (SELECT TOP 1 batch_id FROM dbo.ukg_import_log ORDER BY created_at DESC);
```

---

## 📁 Estructura del proyecto

```
ukg-oracle-etl/
├── src/
│   ├── crypto_utils.py    # AES-256-GCM encrypt/decrypt
│   ├── ukg_loader.py      # CSV → staging (streaming)
│   └── ukg_pipeline.py    # Orchestrator: load → merge
├── sql/
│   ├── 01_staging_tables.sql    # Tablas staging
│   └── 02_merge_procedure.sql   # MERGE procedure
├── tests/
│   └── fixtures/
│       └── sample_employees.csv # 5 empleados de prueba
├── logs/                  # Cron output
├── requirements.txt       # pyodbc + cryptography
└── README.md
```

---

## 🔒 Compliance Legal

### Ley 126-2012 — Firma Electrónica
- SHA-256 hash chain audit por cada batch de importación
- Cada lote genera un hash criptográfico que encadena todas las filas procesadas
- Non-repudiation: el hash chain prueba que los datos no fueron alterados post-importación

### 🔐 Encriptación AES-256-GCM (SSN)
- **SSN se encripta en la capa de aplicación ANTES de tocar la base de datos.**
- Algoritmo: AES-256-GCM (Galois/Counter Mode) — autenticado, con integridad.
- Nonce: 96 bits aleatorios por cada encriptación (nunca se reusa).
- Ciphertext: `base64(nonce[12] || ciphertext || tag[16])` → ~88 chars para SSN 11 chars.
- **Plaintext SSN nunca existe en staging ni en target — solo ciphertext.**
- Logs: SSN enmascarado (`XXX-XX-1234`) — nunca plaintext ni ciphertext.

```bash
# Generar llave de encriptación (32 bytes = AES-256)
python -c "import os,base64; print(base64.b64encode(os.urandom(32)).decode())"

# Configurar en el entorno (NUNCA commitear al repo)
export UKG_ENCRYPTION_KEY="PegarAquiLaLlaveGenerada=="

# Sin esta variable, el pipeline aborta con error claro:
# RuntimeError: UKG_ENCRYPTION_KEY not set.
```

**Rotación de llave:**
```python
from crypto_utils import rotate_key
import pyodbc

new_key = "NuevaLlaveBase64=="
conn = pyodbc.connect(UKG_DB_CONN)
rows = rotate_key(new_key, conn)  # Re-encripta todos los SSNs
print(f"{rows} SSNs re-encriptados")
```

### Ley de Protección de Datos
- Data residency: Datos en SQL Server on-premise o VPC controlada
- TLS: pyodbc con `Encrypt=yes` en producción
- Credenciales vía variables de entorno, nunca en código

---

## 📝 Licencia

Internal — Gobierno de Puerto Rico
