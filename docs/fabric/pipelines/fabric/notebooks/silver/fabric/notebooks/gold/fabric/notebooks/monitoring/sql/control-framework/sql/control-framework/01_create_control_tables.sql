/*==============================================================================
  BritMart Retail Data Platform
  Corrected Fabric Warehouse control framework

  The ctl schema already exists.
  This script creates six control tables and adds informational constraints
  separately using ALTER TABLE.
==============================================================================*/


/*==============================================================================
  1. SOURCE SYSTEM
==============================================================================*/

CREATE TABLE ctl.source_system
(
    source_system_id       UNIQUEIDENTIFIER NOT NULL,
    source_system_code     VARCHAR(50)      NOT NULL,
    source_system_name     VARCHAR(200)     NOT NULL,
    source_type            VARCHAR(50)      NOT NULL,
    connection_name        VARCHAR(200)     NOT NULL,
    source_description     VARCHAR(500)     NULL,
    source_owner           VARCHAR(200)     NULL,
    default_load_type      VARCHAR(30)      NOT NULL,
    execution_group        INT              NOT NULL,
    active_flag            BIT              NOT NULL,
    created_at_utc         DATETIME2(6)     NOT NULL,
    updated_at_utc         DATETIME2(6)     NOT NULL
);
GO


/*==============================================================================
  2. INGESTION CONFIGURATION
==============================================================================*/

CREATE TABLE ctl.ingestion_config
(
    ingestion_config_id        UNIQUEIDENTIFIER NOT NULL,
    source_system_id           UNIQUEIDENTIFIER NOT NULL,

    entity_code                VARCHAR(100)     NOT NULL,
    entity_name                VARCHAR(200)     NOT NULL,
    entity_description         VARCHAR(500)     NULL,

    child_pipeline_name        VARCHAR(200)     NOT NULL,
    load_type                  VARCHAR(30)      NOT NULL,
    load_sequence              INT              NOT NULL,
    parallel_execution_flag    BIT              NOT NULL,

    source_object_name         VARCHAR(300)     NULL,
    source_relative_path       VARCHAR(1000)    NULL,
    source_query               VARCHAR(2000)    NULL,
    source_file_pattern        VARCHAR(300)     NULL,
    source_file_format         VARCHAR(30)      NULL,

    api_page_size              INT              NULL,
    api_pagination_type        VARCHAR(50)      NULL,

    watermark_column_name      VARCHAR(200)     NULL,
    watermark_data_type        VARCHAR(50)      NULL,
    tie_breaker_column_name    VARCHAR(200)     NULL,
    overlap_minutes            INT              NULL,

    primary_key_columns        VARCHAR(500)     NOT NULL,
    expected_schema_version    INT              NOT NULL,

    bronze_lakehouse_name      VARCHAR(200)     NOT NULL,
    bronze_directory           VARCHAR(1000)    NOT NULL,
    bronze_file_format         VARCHAR(30)      NOT NULL,

    retry_count                INT              NOT NULL,
    retry_interval_seconds     INT              NOT NULL,
    timeout_minutes            INT              NOT NULL,

    reconciliation_required    BIT              NOT NULL,
    active_flag                BIT              NOT NULL,

    created_at_utc             DATETIME2(6)     NOT NULL,
    updated_at_utc             DATETIME2(6)     NOT NULL
);
GO


/*==============================================================================
  3. WATERMARK TRACKER
==============================================================================*/

CREATE TABLE ctl.watermark_tracker
(
    watermark_id                  UNIQUEIDENTIFIER NOT NULL,
    ingestion_config_id           UNIQUEIDENTIFIER NOT NULL,

    last_successful_watermark     VARCHAR(500)     NULL,
    last_successful_tie_breaker   VARCHAR(500)     NULL,

    candidate_watermark           VARCHAR(500)     NULL,
    candidate_tie_breaker         VARCHAR(500)     NULL,

    last_successful_run_id        UNIQUEIDENTIFIER NULL,
    last_successful_at_utc        DATETIME2(6)     NULL,

    watermark_status              VARCHAR(30)      NOT NULL,
    created_at_utc                DATETIME2(6)     NOT NULL,
    updated_at_utc                DATETIME2(6)     NOT NULL
);
GO


/*==============================================================================
  4. PROCESSED SOURCE OBJECT

  Used for SharePoint and Amazon S3 files.
==============================================================================*/

CREATE TABLE ctl.processed_source_object
(
    processed_object_id          UNIQUEIDENTIFIER NOT NULL,
    ingestion_config_id          UNIQUEIDENTIFIER NOT NULL,

    source_object_path           VARCHAR(1500)    NOT NULL,
    source_object_name           VARCHAR(500)     NOT NULL,
    source_object_version        VARCHAR(300)     NULL,
    source_last_modified_at_utc  DATETIME2(6)     NULL,
    source_content_length        BIGINT           NULL,
    source_content_hash          VARCHAR(256)     NULL,

    processing_status            VARCHAR(30)      NOT NULL,
    first_processed_run_id       UNIQUEIDENTIFIER NULL,
    last_processed_run_id        UNIQUEIDENTIFIER NULL,
    first_processed_at_utc       DATETIME2(6)     NULL,
    last_processed_at_utc        DATETIME2(6)     NULL,

    created_at_utc               DATETIME2(6)     NOT NULL,
    updated_at_utc               DATETIME2(6)     NOT NULL
);
GO


/*==============================================================================
  5. PIPELINE RUN
==============================================================================*/

CREATE TABLE ctl.pipeline_run
(
    pipeline_run_id             UNIQUEIDENTIFIER NOT NULL,
    fabric_pipeline_run_id      VARCHAR(200)     NULL,

    pipeline_name               VARCHAR(200)     NOT NULL,
    trigger_type                VARCHAR(50)      NOT NULL,
    trigger_name                VARCHAR(200)     NULL,

    requested_load_type         VARCHAR(30)      NULL,
    window_start_utc            DATETIME2(6)     NULL,
    window_end_utc              DATETIME2(6)     NULL,

    run_status                  VARCHAR(30)      NOT NULL,
    started_at_utc              DATETIME2(6)     NOT NULL,
    completed_at_utc            DATETIME2(6)     NULL,
    duration_seconds            BIGINT           NULL,

    entities_requested          INT              NOT NULL,
    entities_succeeded          INT              NOT NULL,
    entities_failed             INT              NOT NULL,
    entities_skipped            INT              NOT NULL,

    error_code                  VARCHAR(200)     NULL,
    error_message               VARCHAR(4000)    NULL,
    initiated_by                VARCHAR(300)     NULL,

    created_at_utc              DATETIME2(6)     NOT NULL,
    updated_at_utc              DATETIME2(6)     NOT NULL
);
GO


/*==============================================================================
  6. ENTITY RUN
==============================================================================*/

CREATE TABLE ctl.entity_run
(
    entity_run_id               UNIQUEIDENTIFIER NOT NULL,
    pipeline_run_id             UNIQUEIDENTIFIER NOT NULL,
    ingestion_config_id         UNIQUEIDENTIFIER NOT NULL,

    activity_run_id             VARCHAR(200)     NULL,
    entity_code                 VARCHAR(100)     NOT NULL,
    load_type                   VARCHAR(30)      NOT NULL,

    extraction_start_value      VARCHAR(300)     NULL,
    extraction_end_value        VARCHAR(300)     NULL,
    extraction_tie_breaker      VARCHAR(300)     NULL,

    source_object_count         BIGINT           NULL,
    source_row_count            BIGINT           NULL,
    bronze_row_count            BIGINT           NULL,
    rejected_row_count          BIGINT           NULL,
    bytes_read                  BIGINT           NULL,
    bytes_written               BIGINT           NULL,

    destination_path            VARCHAR(1000)    NULL,
    run_status                  VARCHAR(30)      NOT NULL,
    attempt_number              INT              NOT NULL,

    started_at_utc              DATETIME2(6)     NOT NULL,
    completed_at_utc            DATETIME2(6)     NULL,
    duration_seconds            BIGINT           NULL,

    error_code                  VARCHAR(200)     NULL,
    error_category              VARCHAR(100)     NULL,
    error_message               VARCHAR(4000)    NULL,

    created_at_utc              DATETIME2(6)     NOT NULL,
    updated_at_utc              DATETIME2(6)     NOT NULL
);
GO


/*==============================================================================
  ADD INFORMATIONAL KEY CONSTRAINTS

  Fabric Warehouse supports these constraints only as NOT ENFORCED.
==============================================================================*/

ALTER TABLE ctl.source_system
ADD CONSTRAINT PK_ctl_source_system
PRIMARY KEY NONCLUSTERED (source_system_id) NOT ENFORCED;
GO

ALTER TABLE ctl.source_system
ADD CONSTRAINT UQ_ctl_source_system_code
UNIQUE NONCLUSTERED (source_system_code) NOT ENFORCED;
GO


ALTER TABLE ctl.ingestion_config
ADD CONSTRAINT PK_ctl_ingestion_config
PRIMARY KEY NONCLUSTERED (ingestion_config_id) NOT ENFORCED;
GO

ALTER TABLE ctl.ingestion_config
ADD CONSTRAINT UQ_ctl_ingestion_entity
UNIQUE NONCLUSTERED
(
    source_system_id,
    entity_code
) NOT ENFORCED;
GO

ALTER TABLE ctl.ingestion_config
ADD CONSTRAINT FK_ctl_ingestion_source_system
FOREIGN KEY (source_system_id)
REFERENCES ctl.source_system(source_system_id)
NOT ENFORCED;
GO


ALTER TABLE ctl.watermark_tracker
ADD CONSTRAINT PK_ctl_watermark_tracker
PRIMARY KEY NONCLUSTERED (watermark_id) NOT ENFORCED;
GO

ALTER TABLE ctl.watermark_tracker
ADD CONSTRAINT UQ_ctl_entity_watermark
UNIQUE NONCLUSTERED (ingestion_config_id) NOT ENFORCED;
GO

ALTER TABLE ctl.watermark_tracker
ADD CONSTRAINT FK_ctl_watermark_ingestion
FOREIGN KEY (ingestion_config_id)
REFERENCES ctl.ingestion_config(ingestion_config_id)
NOT ENFORCED;
GO


ALTER TABLE ctl.processed_source_object
ADD CONSTRAINT PK_ctl_processed_source_object
PRIMARY KEY NONCLUSTERED (processed_object_id) NOT ENFORCED;
GO

ALTER TABLE ctl.processed_source_object
ADD CONSTRAINT FK_ctl_object_ingestion
FOREIGN KEY (ingestion_config_id)
REFERENCES ctl.ingestion_config(ingestion_config_id)
NOT ENFORCED;
GO


ALTER TABLE ctl.pipeline_run
ADD CONSTRAINT PK_ctl_pipeline_run
PRIMARY KEY NONCLUSTERED (pipeline_run_id) NOT ENFORCED;
GO


ALTER TABLE ctl.entity_run
ADD CONSTRAINT PK_ctl_entity_run
PRIMARY KEY NONCLUSTERED (entity_run_id) NOT ENFORCED;
GO

ALTER TABLE ctl.entity_run
ADD CONSTRAINT FK_ctl_entity_pipeline_run
FOREIGN KEY (pipeline_run_id)
REFERENCES ctl.pipeline_run(pipeline_run_id)
NOT ENFORCED;
GO

ALTER TABLE ctl.entity_run
ADD CONSTRAINT FK_ctl_entity_ingestion
FOREIGN KEY (ingestion_config_id)
REFERENCES ctl.ingestion_config(ingestion_config_id)
NOT ENFORCED;
GO


/*==============================================================================
  FINAL VERIFICATION

  The success message appears only when all six tables exist.
==============================================================================*/

DECLARE @table_count INT;

SELECT
    @table_count = COUNT(*)
FROM sys.tables AS table_object
INNER JOIN sys.schemas AS schema_object
    ON schema_object.schema_id = table_object.schema_id
WHERE schema_object.name = 'ctl';

SELECT
    result =
        CASE
            WHEN @table_count = 6
                THEN 'SUCCESS: All six BritMart control tables were created.'
            ELSE
                'FAILED: The expected six control tables were not created.'
        END,
    table_count = @table_count;
GO


SELECT
    schema_name = schema_object.name,
    table_name = table_object.name
FROM sys.tables AS table_object
INNER JOIN sys.schemas AS schema_object
    ON schema_object.schema_id = table_object.schema_id
WHERE schema_object.name = 'ctl'
ORDER BY table_object.name;
GO
