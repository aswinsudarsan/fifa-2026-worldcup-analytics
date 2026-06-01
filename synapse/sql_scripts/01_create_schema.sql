-- ============================================================
-- Script: 01_create_schema.sql
-- Pool:   Dedicated SQL Pool (sqldw_fifa2026)
-- Purpose: Create database schemas and control tables
-- ============================================================

-- Create schemas aligned with Medallion layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS reporting;
GO

-- Pipeline execution log
CREATE TABLE dbo.pipeline_execution_log (
    log_id          INT IDENTITY(1,1),
    pipeline_name   NVARCHAR(200)   NOT NULL,
    layer           NVARCHAR(20)    NOT NULL,   -- bronze / silver / gold / ml
    status          NVARCHAR(20)    NOT NULL,   -- running / success / failed
    started_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
    completed_at    DATETIME2       NULL,
    rows_processed  BIGINT          NULL,
    error_message   NVARCHAR(MAX)   NULL,
    triggered_by    NVARCHAR(100)   NULL        -- adf / manual / schedule
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO

-- Data quality log
CREATE TABLE dbo.data_quality_log (
    dq_id           INT IDENTITY(1,1),
    check_date      DATE            NOT NULL DEFAULT CAST(GETUTCDATE() AS DATE),
    table_name      NVARCHAR(200)   NOT NULL,
    check_name      NVARCHAR(200)   NOT NULL,
    total_rows      BIGINT,
    failed_rows     BIGINT,
    pass_rate       DECIMAL(5,4),
    status          NVARCHAR(10)    -- PASS / FAIL / WARN
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO
