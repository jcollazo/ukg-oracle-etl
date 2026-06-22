-- ============================================================
-- 02_merge_procedure.sql — MERGE staging → target tables
-- UKG → Oracle ETL — Phase 1 (SQL Staging approach)
-- ============================================================
-- Atomic MERGE: inserta nuevos empleados, actualiza existentes.
-- Idempotente: correr 2 veces con los mismos datos → mismo resultado.
--
-- ⚠️ CONFIGURACIÓN REQUERIDA antes de deploy:
--    Las tablas destino (dbo.empleados, dbo.agencias, dbo.puestos)
--    y sus columnas deben adaptarse al schema de la base de datos
--    objetivo. Ver sección "TARGET SCHEMA" abajo.
-- ============================================================
--
-- TARGET SCHEMA (customize for your database):
--   dbo.agencias  → agencias / departments
--     Columnas requeridas: id (PK), codigo, nombre
--     Columnas opcionales: codigo_prifas, activa
--   dbo.puestos   → puestos / positions / jobs
--     Columnas requeridas: id (PK), codigo_clase, titulo, agencia_id (FK)
--   dbo.empleados → empleados / employees
--     Columnas requeridas: id (PK), numero_empleado, nombre,
--       apellido_paterno, agencia_id (FK), estado_empleado
--     Columnas opcionales: apellido_materno, email_institucional,
--       telefono, puesto_actual_id (FK), fecha_ingreso, activo
-- ============================================================

CREATE OR ALTER PROCEDURE dbo.ukg_merge_employees
    @batch_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;  -- Rollback on any error — Ley 126-2012 compliance

    DECLARE @inserted INT = 0, @updated INT = 0;
    DECLARE @batch_hash NVARCHAR(64);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- ─── Step 1: Resolve / create agencies ──────────────
        -- NOTE: Customize INSERT column list to match your agencias table
        MERGE INTO dbo.agencias AS target
        USING (
            SELECT DISTINCT 
                dept_code,
                dept_name
            FROM dbo.ukg_staging
            WHERE batch_id = @batch_id
        ) AS source
        ON target.codigo = source.dept_code
        WHEN NOT MATCHED THEN
            INSERT (codigo, nombre, activa)
            VALUES (source.dept_code, source.dept_name, 1);

        -- ─── Step 2: Resolve / create positions ─────────────
        -- NOTE: Customize INSERT column list to match your puestos table
        MERGE INTO dbo.puestos AS target
        USING (
            SELECT DISTINCT
                s.position_code,
                s.position_title,
                a.id AS agencia_id
            FROM dbo.ukg_staging s
            JOIN dbo.agencias a ON a.codigo = s.dept_code
            WHERE s.batch_id = @batch_id
              AND s.position_code IS NOT NULL
        ) AS source
        ON target.codigo_clase = source.position_code
           AND target.agencia_id = source.agencia_id
        WHEN NOT MATCHED THEN
            INSERT (codigo_clase, titulo, agencia_id)
            VALUES (source.position_code, source.position_title, source.agencia_id);

        -- ─── Step 3: MERGE employees (the big one) ──────────
        -- NOTE: Customize column names to match your empleados table
        -- SSN: encrypt before storing (AES-256 recommended).  
        --       This procedure stores the plaintext SSN in the target
        --       table. Production deployments should encrypt at the
        --       application layer or via Always Encrypted.
        MERGE INTO dbo.empleados AS target
        USING (
            SELECT
                s.eeid,
                s.ssn,
                s.first_name,
                s.last_name,
                s.middle_name,
                s.email,
                s.phone,
                a.id AS agencia_id,
                p.id AS puesto_id,
                s.hire_date,
                CASE 
                    WHEN s.employee_status = 'ACTIVE'   THEN 'ACTIVO'
                    WHEN s.employee_status = 'TERMINATED' THEN 'TERMINADO'
                    WHEN s.employee_status = 'ON_LEAVE'  THEN 'LICENCIA'
                    ELSE 'ACTIVO'
                END AS estado_empleado,
                s.raw_line
            FROM dbo.ukg_staging s
            JOIN dbo.agencias a ON a.codigo = s.dept_code
            LEFT JOIN dbo.puestos p ON p.codigo_clase = s.position_code 
                                    AND p.agencia_id = a.id
            WHERE s.batch_id = @batch_id
        ) AS source
        ON target.numero_empleado = source.eeid
           AND target.agencia_id = source.agencia_id
        WHEN MATCHED THEN
            UPDATE SET
                ssn              = source.ssn,  -- encrypt in production
                nombre           = source.first_name,
                apellido_paterno = source.last_name,
                apellido_materno = source.middle_name,
                email_institucional = source.email,
                telefono         = source.phone,
                puesto_actual_id = COALESCE(source.puesto_id, target.puesto_actual_id),
                fecha_ingreso    = COALESCE(source.hire_date, target.fecha_ingreso),
                estado_empleado  = source.estado_empleado,
                activo           = CASE WHEN source.estado_empleado = 'TERMINADO' THEN 0 ELSE 1 END
        WHEN NOT MATCHED THEN
            INSERT (ssn, numero_empleado, nombre, apellido_paterno, apellido_materno,
                    email_institucional, telefono, agencia_id, puesto_actual_id,
                    fecha_ingreso, estado_empleado, activo)
                    VALUES (source.ssn, source.eeid, source.first_name, source.last_name,
                    source.middle_name, source.email, source.phone,
                    source.agencia_id, source.puesto_id,
                    source.hire_date, source.estado_empleado, 1);

        SET @inserted = @@ROWCOUNT;

        -- ─── Step 4: Generate audit hash (Ley 126-2012) ─────
        SELECT @batch_hash = CONVERT(NVARCHAR(64), 
            HASHBYTES('SHA2_256', 
                STRING_AGG(CONVERT(NVARCHAR(MAX), 
                    HASHBYTES('SHA2_256', ISNULL(raw_line, '')), 2
                ), '') 
            ), 2)
        FROM dbo.ukg_staging
        WHERE batch_id = @batch_id;

        -- ─── Step 5: Update import log ──────────────────────
        UPDATE dbo.ukg_import_log
        SET status = 'COMPLETED',
            inserted_rows = @inserted,
            import_hash = @batch_hash,
            completed_at = SYSUTCDATETIME()
        WHERE batch_id = @batch_id;

        COMMIT TRANSACTION;

        SELECT 
            @batch_id AS batch_id,
            'COMPLETED' AS status,
            @inserted AS rows_affected,
            @batch_hash AS audit_hash;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        UPDATE dbo.ukg_import_log
        SET status = 'FAILED',
            error_summary = CONCAT(
                'Error ', ERROR_NUMBER(), ': ', ERROR_MESSAGE(),
                ' (Line ', ERROR_LINE(), ', Procedure: ', ERROR_PROCEDURE(), ')'
            ),
            completed_at = SYSUTCDATETIME()
        WHERE batch_id = @batch_id;

        THROW;
    END CATCH
END
GO

PRINT '✓ MERGE procedure created';
