-- ============================================================
-- Script: 04_stored_procedures.sql
-- Pool:   Dedicated SQL Pool (sqldw_fifa2026)
-- Purpose: ETL stored procedures for loading Gold → SQL Pool
-- ============================================================

-- ── Load dim_team from external table ────────────────────────────────────────
CREATE OR ALTER PROCEDURE reporting.usp_load_dim_team
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('reporting.dim_team', 'U') IS NOT NULL
        DROP TABLE reporting.dim_team;

    CREATE TABLE reporting.dim_team
    WITH (
        DISTRIBUTION = REPLICATE,    -- small dimension table
        CLUSTERED INDEX (team_name)
    )
    AS
    SELECT *
    FROM gold.dim_team;

    INSERT INTO dbo.pipeline_execution_log (pipeline_name, layer, status, rows_processed, triggered_by)
    SELECT 'usp_load_dim_team', 'gold', 'success', COUNT(*), 'sql_proc'
    FROM reporting.dim_team;

    PRINT 'dim_team loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(20)) + ' rows';
END;
GO

-- ── Load fact_matches from external table ─────────────────────────────────────
CREATE OR ALTER PROCEDURE reporting.usp_load_fact_matches
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('reporting.fact_matches', 'U') IS NOT NULL
        DROP TABLE reporting.fact_matches;

    CREATE TABLE reporting.fact_matches
    WITH (
        DISTRIBUTION = HASH(match_id),
        CLUSTERED COLUMNSTORE INDEX,
        PARTITION (match_year RANGE RIGHT FOR VALUES (
            2000, 2002, 2006, 2010, 2014, 2018, 2022, 2024, 2025, 2026
        ))
    )
    AS
    SELECT *
    FROM gold.fact_matches;

    INSERT INTO dbo.pipeline_execution_log (pipeline_name, layer, status, rows_processed, triggered_by)
    SELECT 'usp_load_fact_matches', 'gold', 'success', COUNT(*), 'sql_proc'
    FROM reporting.fact_matches;

    PRINT 'fact_matches loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(20)) + ' rows';
END;
GO

-- ── Load WC 2026 predictions ──────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE reporting.usp_load_wc2026_predictions
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('reporting.wc2026_predictions', 'U') IS NOT NULL
        DROP TABLE reporting.wc2026_predictions;

    CREATE TABLE reporting.wc2026_predictions
    WITH (
        DISTRIBUTION = REPLICATE,
        HEAP
    )
    AS
    SELECT * FROM gold.wc2026_group_predictions;

    PRINT 'wc2026_predictions loaded.';
END;
GO

-- ── Master ETL procedure ──────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE reporting.usp_run_full_load
AS
BEGIN
    DECLARE @start DATETIME2 = GETUTCDATE();

    BEGIN TRY
        EXEC reporting.usp_load_dim_team;
        EXEC reporting.usp_load_fact_matches;
        EXEC reporting.usp_load_wc2026_predictions;

        PRINT 'Full load completed in ' +
              CAST(DATEDIFF(SECOND, @start, GETUTCDATE()) AS NVARCHAR) + 's';
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.pipeline_execution_log
            (pipeline_name, layer, status, error_message, triggered_by)
        VALUES ('usp_run_full_load', 'reporting', 'failed',
                ERROR_MESSAGE(), 'sql_proc');

        THROW;
    END CATCH;
END;
GO

-- ── Data quality checks ───────────────────────────────────────────────────────
CREATE OR ALTER PROCEDURE reporting.usp_run_dq_checks
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @check_date DATE = CAST(GETUTCDATE() AS DATE);

    -- Check 1: No null match_id in fact_matches
    INSERT INTO dbo.data_quality_log (check_date, table_name, check_name, total_rows, failed_rows, pass_rate, status)
    SELECT @check_date, 'fact_matches', 'null_match_id',
           COUNT(*), SUM(CASE WHEN match_id IS NULL THEN 1 ELSE 0 END),
           1.0 - (SUM(CASE WHEN match_id IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*)),
           CASE WHEN SUM(CASE WHEN match_id IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM gold.fact_matches;

    -- Check 2: Scores are non-negative
    INSERT INTO dbo.data_quality_log (check_date, table_name, check_name, total_rows, failed_rows, pass_rate, status)
    SELECT @check_date, 'fact_matches', 'negative_scores',
           COUNT(*), SUM(CASE WHEN home_score < 0 OR away_score < 0 THEN 1 ELSE 0 END),
           1.0 - (SUM(CASE WHEN home_score < 0 OR away_score < 0 THEN 1.0 ELSE 0.0 END) / COUNT(*)),
           CASE WHEN SUM(CASE WHEN home_score < 0 OR away_score < 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM gold.fact_matches;

    -- Check 3: 48 qualified teams in WC table
    INSERT INTO dbo.data_quality_log (check_date, table_name, check_name, total_rows, failed_rows, pass_rate, status)
    SELECT @check_date, 'wc2026_qualified_teams', 'team_count_48',
           COUNT(*), ABS(COUNT(*) - 48),
           CASE WHEN COUNT(*) = 48 THEN 1.0 ELSE 0.0 END,
           CASE WHEN COUNT(*) = 48 THEN 'PASS' ELSE 'WARN' END
    FROM gold.wc2026_qualified_teams;

    SELECT * FROM dbo.data_quality_log
    WHERE check_date = @check_date
    ORDER BY status DESC, table_name;
END;
GO

PRINT 'Stored procedures created.';
