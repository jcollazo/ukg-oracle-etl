# ============================================================
# ukg_loader.py — CSV → SQL Server staging (streaming, ~10 MB RAM)
# UKG → Oracle ETL — Phase 1 (SQL Staging approach)
# ============================================================
# Uso:
#   python ukg_loader.py /sftp/employees_2026-06-22.csv
# Cron:
#   0 2 * * * python /opt/ukg-oracle-etl/src/ukg_loader.py /sftp/latest.csv
# ============================================================
import csv
import hashlib
import logging
import os
import sys
import uuid
from datetime import UTC, datetime

import pyodbc

# ─── Config ──────────────────────────────────────────────────
DB_CONN = os.getenv("UKG_DB_CONN", 
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;DATABASE=HR_OATRH;UID=sa;PWD=YourStrongPassw0rd;"
    "Encrypt=no;TrustServerCertificate=yes;"
)
BATCH_SIZE = 1000  # Rows per INSERT batch
EXPECTED_COLUMNS = 20  # UKG CSV columns

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ukg-loader")

# ─── Main ────────────────────────────────────────────────────
def load_csv_to_staging(csv_path: str) -> dict:
    """Stream CSV into dbo.ukg_staging table. Returns batch summary."""
    filename = os.path.basename(csv_path)
    batch_id = str(uuid.uuid4())
    total_rows = 0
    error_rows = 0

    conn = pyodbc.connect(DB_CONN, autocommit=False)
    cursor = conn.cursor()

    # Create import log entry
    cursor.execute(
        "INSERT INTO dbo.ukg_import_log (batch_id, filename, status, started_at) "
        "VALUES (?, ?, 'STAGING_LOADING', SYSUTCDATETIME())",
        batch_id, filename
    )
    conn.commit()
    logger.info("Batch %s — Loading %s", batch_id[:8], filename)

    try:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            # Normalize header names (strip whitespace, BOM)
            reader.fieldnames = [h.strip().replace("\ufeff", "") for h in reader.fieldnames]

            batch = []
            for row_num, row in enumerate(reader, start=1):
                try:
                    batch.append((
                        batch_id, row_num,
                        row.get("EEID", ""),
                        row.get("FirstName", ""),
                        row.get("LastName", ""),
                        row.get("MiddleName", ""),
                        row.get("PositionCode", ""),
                        row.get("PositionTitle", ""),
                        row.get("DepartmentCode", ""),
                        row.get("DepartmentName", ""),
                        row.get("CostCenter", ""),
                        parse_date(row.get("HireDate")),
                        parse_date(row.get("RehireDate")),
                        parse_date(row.get("TerminationDate")),
                        row.get("Status", "ACTIVE"),
                        row.get("Email", ""),
                        row.get("Phone", ""),
                        row.get("SupervisorName", ""),
                        row.get("SupervisorEmail", ""),
                        parse_decimal(row.get("PayRate")),
                        row.get("PayType", ""),
                        row.get("FLSAStatus", ""),
                        row.get("LocationCode", ""),
                        str(row)  # raw_line for audit
                    ))
                except Exception as exc:
                    log_error(cursor, batch_id, row_num, row.get("EEID"), str(exc), str(row))
                    error_rows += 1

                total_rows += 1

                # Flush batch every BATCH_SIZE rows
                if len(batch) >= BATCH_SIZE:
                    flush_batch(cursor, batch)
                    # Save checkpoint
                    save_checkpoint(cursor, batch_id, row_num)
                    conn.commit()
                    logger.info("  …%d rows loaded", row_num)
                    batch = []

            # Flush final batch
            if batch:
                flush_batch(cursor, batch)
                save_checkpoint(cursor, batch_id, total_rows)
                conn.commit()

    except Exception as exc:
        conn.rollback()
        cursor.execute(
            "UPDATE dbo.ukg_import_log SET status='FAILED', error_summary=?, completed_at=SYSUTCDATETIME() "
            "WHERE batch_id=?",
            str(exc), batch_id
        )
        conn.commit()
        logger.exception("FATAL: %s", exc)
        raise
    finally:
        conn.close()

    # Mark staging loaded — MERGE will be called separately
    cursor2 = pyodbc.connect(DB_CONN, autocommit=True).cursor()
    cursor2.execute(
        "UPDATE dbo.ukg_import_log SET status='STAGING_LOADED', total_rows=?, error_rows=? "
        "WHERE batch_id=?",
        total_rows, error_rows, batch_id
    )
    cursor2.close()

    return {
        "batch_id": batch_id,
        "filename": filename,
        "total_rows": total_rows,
        "error_rows": error_rows,
        "status": "STAGING_LOADED"
    }


def flush_batch(cursor, batch):
    """INSERT batch into staging table."""
    cursor.executemany(
        "INSERT INTO dbo.ukg_staging (batch_id, row_number, eeid, first_name, last_name, "
        "middle_name, position_code, position_title, dept_code, dept_name, cost_center, "
        "hire_date, rehire_date, termination_date, employee_status, email, phone, "
        "supervisor_name, supervisor_email, pay_rate, pay_type, flsa_status, "
        "location_code, raw_line) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch
    )


def save_checkpoint(cursor, batch_id, row_num):
    """Save checkpoint for crash recovery."""
    cursor.execute(
        "MERGE INTO dbo.ukg_checkpoint AS target "
        "USING (SELECT ? AS batch_id, ? AS last_row) AS source "
        "ON target.batch_id = source.batch_id "
        "WHEN MATCHED THEN UPDATE SET last_row=source.last_row, updated_at=SYSUTCDATETIME() "
        "WHEN NOT MATCHED THEN INSERT (batch_id, last_row) VALUES (source.batch_id, source.last_row);",
        batch_id, row_num
    )


def log_error(cursor, batch_id, row_num, eeid, error_msg, raw_data):
    """Log row-level error to dead letter table."""
    cursor.execute(
        "INSERT INTO dbo.ukg_error_log (batch_id, row_number, eeid, error_message, raw_data) "
        "VALUES (?, ?, ?, ?, ?)",
        batch_id, row_num, eeid, error_msg[:1000], raw_data[:4000]
    )


def parse_date(val):
    """Parse YYYY-MM-DD date, return None if invalid."""
    if not val or not val.strip():
        return None
    try:
        return datetime.strptime(val.strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_decimal(val):
    """Parse decimal, return None if invalid."""
    if not val or not val.strip():
        return None
    try:
        return float(val.strip().replace("$", "").replace(",", ""))
    except ValueError:
        return None


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Uso: python {sys.argv[0]} <csv_path>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        logger.error("File not found: %s", csv_path)
        sys.exit(1)

    result = load_csv_to_staging(csv_path)
    
    print()
    print("=" * 50)
    print("  STAGING LOAD COMPLETE")
    print("=" * 50)
    print(f"  Batch ID:   {result['batch_id']}")
    print(f"  File:       {result['filename']}")
    print(f"  Total rows: {result['total_rows']}")
    print(f"  Errors:     {result['error_rows']}")
    print(f"  Status:     {result['status']}")
    print()
    print("  Next: EXEC dbo.ukg_merge_employees @batch_id = '{}'".format(result['batch_id']))
    print("=" * 50)
