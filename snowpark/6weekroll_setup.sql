//Create Database if not exists
CREATE DATABASE IF NOT EXISTS dev_db;

USE DATABASE dev_db;

//Create Schema if not exists
CREATE SCHEMA IF NOT EXISTS dev_db.snowpark_lab;

USE SCHEMA dev_db.snowpark_lab;

//Create Storage Integration
CREATE STORAGE INTEGRATION IF NOT EXISTS gcs_st_integration
    TYPE = EXTERNAL_STAGE
    ENABLED = TRUE
    STORAGE_PROVIDER = 'GCS'
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake_bckt02/csv/snowpipe/sales');

DESC STORAGE INTEGRATION gcs_st_integration;

//Create Email Alert integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_alert_integration
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('ng1492000@gmail.com');

DESC NOTIFICATION INTEGRATION email_alert_integration;

//Test if Email Setup is done properly
//CALL SYSTEM$SEND_EMAIL('email_alert_integration', 'ng1492000@gmail.com', 'TEST', 'Hello from Snowflake');

//Create CSV file Format
CREATE FILE FORMAT IF NOT EXISTS csv_file_format
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    ;

//Create Target Table
CREATE TABLE IF NOT EXISTS SALES_ROLLING (
    ORDER_ID    NUMBER,
    PRODUCT     STRING,
    QTY         NUMBER,
    PRICE       FLOAT,
    FISCAL_WEEK NUMBER,
    ORDER_FISCALWEEK STRING,
    WEEK_START  DATE,            -- Monday of the week the file belongs to
    SOURCE_FILE STRING,
    LOAD_TS     TIMESTAMP_NTZ
);

//CREATE Transient table TABLE
CREATE TRANSIENT TABLE IF NOT EXISTS dev_db.public.staged_sales_table(
    ORDER_ID NUMBER,
    PRODUCT  STRING,
    QTY      NUMBER,
    PRICE    FLOAT,
    FISCAL_WEEK NUMBER,
    ORDER_FISCALWEEK STRING
)DATA_RETENTION_TIME_IN_DAYS = 0;

//Create Table for logs and analysis
CREATE TABLE IF NOT EXISTS PIPELINE_LOG (
    RUN_ID        STRING,
    LOG_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PIPELINE      STRING,
    STEP          STRING,
    STATUS        STRING,        -- INFO / OK / WARN / ERROR
    ROWS_AFFECTED NUMBER,
    MESSAGE       STRING
);


CREATE STAGE IF NOT EXISTS gcs_csv_ext_stage
    URL = 'gcs://snowflake_bckt02/csv/snowpipe/sales'
    STORAGE_INTEGRATION = gcs_st_integration
    FILE_FORMAT = csv_file_format
    DIRECTORY = (ENABLE = TRUE)
    ;

LIST @gcs_csv_ext_stage;


//Testing if the External Stage is created properly
//COPY INTO dev_db.public.staged_sales_table
//FROM @dev_db.snowpark_lab.gcs_csv_ext_stage
//ON_ERROR = 'SKIP_FILE_10%';


SELECT COUNT(*) FROM dev_db.public.staged_sales_table;
