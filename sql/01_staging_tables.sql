-- ============================================================
-- 01_staging_tables.sql — UKG Import staging & audit tables
-- UKG → Oracle ETL — Phase 1 (SQL Staging approach)
-- ============================================================
-- Target: SQL Server 2022+
-- ============================================================

-- ─── Staging table (CSV raw data lands here) ────────────────
IF OBJECT_ID('dbo.ukg_staging', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ukg_staging (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        batch_id        UNIQUEIDENTIFIER NOT NULL,
        row_number      INT NOT NULL,
        -- UKG raw fields
        eeid            NVARCHAR(50) NOT NULL,
        first_name      NVARCHAR(100),
        last_name       NVARCHAR(100),
        middle_name     NVARCHAR(100),
        position_code   NVARCHAR(50),
        position_title  NVARCHAR(200),
        dept_code       NVARCHAR(50),
        dept_name       NVARCHAR(200),
        cost_center     NVARCHAR(50),
        hire_date       DATE,
        rehire_date     DATE,
        termination_date DATE,
        employee_status NVARCHAR(30),
        email           NVARCHAR(200),
        phone           NVARCHAR(50),
        supervisor_name NVARCHAR(200),
        supervisor_email NVARCHAR(200),
        pay_rate        DECIMAL(10,2),
        pay_type        NVARCHAR(20),
        flsa_status     NVARCHAR(20),
        location_code   NVARCHAR(50),
        -- Metadata
        raw_line        NVARCHAR(MAX),
        imported_at     DATETIME2 DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_ukg_staging_batch ON dbo.ukg_staging(batch_id);
    CREATE INDEX IX_ukg_staging_eeid   ON dbo.ukg_staging(eeid);
END
GO

-- ─── Import log (one row per batch) ────────────────────────
IF OBJECT_ID('dbo.ukg_import_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ukg_import_log (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        batch_id        UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(),
        filename        NVARCHAR(500) NOT NULL,
        status          NVARCHAR(20) NOT NULL DEFAULT 'PENDING',
                        -- PENDING → STAGING_LOADED → MERGED → COMPLETED
                        -- PENDING → FAILED
        total_rows      INT DEFAULT 0,
        inserted_rows   INT DEFAULT 0,
        updated_rows    INT DEFAULT 0,
        error_rows      INT DEFAULT 0,
        import_hash     NVARCHAR(64),  -- SHA-256 audit hash
        started_at      DATETIME2,
        completed_at    DATETIME2,
        error_summary   NVARCHAR(MAX), -- JSON
        created_at      DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

-- ─── Dead letter / error log ───────────────────────────────
IF OBJECT_ID('dbo.ukg_error_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ukg_error_log (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        batch_id        UNIQUEIDENTIFIER NOT NULL,
        row_number      INT NOT NULL,
        eeid            NVARCHAR(50),
        error_message   NVARCHAR(1000),
        raw_data        NVARCHAR(MAX),
        created_at      DATETIME2 DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_ukg_error_batch ON dbo.ukg_error_log(batch_id);
END
GO

-- ─── Checkpoint table (resume after crash) ─────────────────
IF OBJECT_ID('dbo.ukg_checkpoint', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ukg_checkpoint (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        batch_id        UNIQUEIDENTIFIER NOT NULL,
        last_row        INT NOT NULL DEFAULT 0,
        chunk_hash      NVARCHAR(64),
        updated_at      DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

PRINT '✓ Staging tables created successfully';
