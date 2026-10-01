//Create Database
CREATE OR REPLACE DATABASE SNOWPIPE_TEST_DB;


//Create Schema for Storage Integration
CREATE OR REPLACE SCHEMA ST_INTEGRATION;


//Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION gcp_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake_bckt02/csv/snowpipe/');

DESC STORAGE INTEGRATION gcp_integration;


//projects/snowflake-project-510103/topics/snowflake_topic02
//Create Schema for Notification Integration
CREATE OR REPLACE SCHEMA NOTIFY_INTEGRATION;

//Create Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION gcp_notif_integration
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/snowflake-project-510103/subscriptions/snowflake_topic02-sub';

DESC NOTIFICATION INTEGRATION gcp_notif_integration;

CREATE OR REPLACE SCHEMA file_formats;

// Create file format object
CREATE OR REPLACE file format SNOWPIPE_TEST_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;


CREATE OR REPLACE SCHEMA EXT_STAGE;

CREATE OR REPLACE STAGE SNOWPIPE_TEST_DB.EXT_STAGE.GCP_STAGE
    URL = 'gcs://snowflake_bckt02/csv/snowpipe/'
    STORAGE_INTEGRATION = gcp_integration
    FILE_FORMAT = SNOWPIPE_TEST_DB.file_formats.csv_fileformat
    directory = (enable = True);

LIST @SNOWPIPE_TEST_DB.EXT_STAGE.GCP_STAGE;

CREATE OR REPLACE SCHEMA STAGED_TABLE;


// Create table first
CREATE OR REPLACE TABLE employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  );

GRANT USAGE ON INTEGRATION GCP_NOTIF_INTEGRATION to ROLE ACCOUNTADMIN;


CREATE OR REPLACE SCHEMA PIPES;

CREATE OR REPLACE PIPE employee_pipe
    AUTO_INGEST = TRUE
    INTEGRATION = 'GCP_NOTIF_INTEGRATION'
    AS
    COPY INTO SNOWPIPE_TEST_DB.STAGED_TABLE.EMPLOYEES
    FROM @SNOWPIPE_TEST_DB.EXT_STAGE.GCP_STAGE ;

ALTER PIPE employee_pipe REFRESH;

//ALTER PIPE SNOWPIPE_TEST_DB.NOTIFY_INTEGRATION.EMPLOYEE_PIPE SET PIPE_EXECUTION_PAUSED= TRUE;
