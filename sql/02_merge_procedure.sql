-- ============================================================
-- 02_merge_procedure.sql — MERGE staging → target tables
-- UKG → Oracle ETL — Phase 1 (SQL Staging approach)
-- ============================================================
-- Atomic MERGE: inserta nuevos empleados, actualiza existentes.
-- Idempotente: correr 2 veces con los mismos datos → mismo resultado.
-- ============================================================

CREATE OR ALTER PROCEDURE dbo.ukg_merge_employees
    @batch_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;  -- Rollback on any error

    DECLARE @inserted INT = 0, @updated INT = 0, @errors INT = 0;
    DECLARE @batch_hash NVARCHAR(64);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- ─── Step 1: Resolve / create agencies ──────────────
        MERGE INTO dbo.agencias AS target
        USING (
            SELECT DISTINCT 
                dept_code,
                dept_name,
                LEFT(dept_code, 3) AS codigo_prifas_candidate
            FROM dbo.ukg_staging
            WHERE batch_id = @batch_id
        ) AS source
        ON target.codigo = source.dept_code
        WHEN NOT MATCHED THEN
            INSERT (codigo, codigo_prifas, nombre, activa)
            VALUES (
                source.dept_code,
                -- Ensure unique codigo_prifas (append counter if collision)
                LEFT(source.dept_code, 3) + 
                    CASE WHEN EXISTS(SELECT 1 FROM dbo.agencias WHERE codigo_prifas = LEFT(source.dept_code, 3))
                    THEN RIGHT('00' + CAST(ROW_NUMBER() OVER(ORDER BY source.dept_code) AS NVARCHAR(2)), 2)
                    ELSE '' END,
                source.dept_name,
                1
            );

        -- ─── Step 2: Resolve / create positions ─────────────
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
        MERGE INTO dbo.empleados AS target
        USING (
            SELECT
                s.eeid,
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
            INSERT (numero_empleado, nombre, apellido_paterno, apellido_materno,
                    email_institucional, telefono, agencia_id, puesto_actual_id,
                    fecha_ingreso, estado_empleado, activo)
            VALUES (source.eeid, source.first_name, source.last_name,
                    source.middle_name, source.email, source.phone,
                    source.agencia_id, source.puesto_id,
                    source.hire_date, source.estado_empleado, 1);

        -- Count affected rows
        SET @inserted = @@ROWCOUNT;

        -- ─── Step 4: Generate audit hash ────────────────────
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

        -- Return summary
        SELECT 
            @batch_id AS batch_id,
            'COMPLETED' AS status,
            @inserted AS rows_affected,
            @batch_hash AS audit_hash;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Log error
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
