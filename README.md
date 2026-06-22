# UKG → Oracle ETL

**Pipeline de integración UKG Pro → base de datos Oracle/SQL Server para el Gobierno de Puerto Rico.**

Phase 1: SQL Staging approach — CSV → staging table → MERGE atómico → audit hash chain.

---

## 🏗️ Arquitectura

```
┌──────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────┐
│ SFTP     │─────▶│ ukg_pipeline │─────▶│ dbo.ukg_     │─────▶│ dbo.     │
│ incoming │      │ .py          │      │ staging      │      │ empleados│
└──────────┘      └──────────────┘      └──────┬───────┘      └──────────┘
                                               │
                                        ┌──────▼───────┐
                                        │ MERGE proc   │ ◀── Atómico
                                        │ (idempotent) │     SQL Server
                                        └──────┬───────┘
                                               │
                                        ┌──────▼───────┐
                                        │ Hash SHA-256 │ ◀── Ley 126
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

### 1. Crear tablas en SQL Server

```bash
sqlcmd -S localhost -U sa -P "YourStrongPassw0rd" -d HR_OATRH \
  -i sql/01_staging_tables.sql \
  -i sql/02_merge_procedure.sql
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Pipeline completo

```bash
# One-shot
python src/ukg_pipeline.py /sftp/employees_2026-06-22.csv

# Solo staging (sin MERGE)
python src/ukg_loader.py /sftp/employees_2026-06-22.csv
```

### 4. Ejecutar MERGE manual

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
| **Idempotente** | MERGE usa `numero_empleado + agencia_id` como key |
| **Atómico** | MERGE en una sola transacción SQL |
| **Rollback** | `SET XACT_ABORT ON` — error → rollback completo |
| **Audit Ley 126** | SHA-256 hash chain por batch en `import_hash` |

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
│   ├── ukg_loader.py      # CSV → staging (streaming)
│   └── ukg_pipeline.py    # Orchestrator: load → merge
├── sql/
│   ├── 01_staging_tables.sql    # Tablas staging
│   └── 02_merge_procedure.sql   # MERGE procedure
├── tests/
│   └── fixtures/
│       └── sample_employees.csv # 5 empleados de prueba
├── logs/                  # Cron output
├── requirements.txt       # Solo pyodbc
└── README.md
```

---

## 🔒 Compliance

- **Ley 126-2012**: SHA-256 hash chain audit por cada batch
- **Data residency**: Datos en SQL Server on-premise/VPC
- **TLS**: pyodbc con Encrypt=yes en producción

---

## 📝 Licencia

Internal — Gobierno de Puerto Rico / OATRH
