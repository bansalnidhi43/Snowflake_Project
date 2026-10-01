//Create Database
CREATE OR REPLACE DATABASE snowpipe_ingest_db;


//Create Schema 
CREATE OR REPLACE SCHEMA ingest;


//Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION gcp_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake_bckt02/csv/snowpipe/');

DESC STORAGE INTEGRATION gcp_integration;

//Create Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION gcp_notif_integration
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/snowflake-project-510103/subscriptions/snowflake_topic02-sub';

DESC NOTIFICATION INTEGRATION gcp_notif_integration;


// Create file format object
CREATE OR REPLACE file format csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;


//Create External Stage Object
CREATE OR REPLACE STAGE gcp_stage
    URL = 'gcs://snowflake_bckt02/csv/snowpipe/'
    STORAGE_INTEGRATION = gcp_integration
    FILE_FORMAT = snowpipe_ingest_db.ingest.csv_fileformat
    directory = (enable = True);

LIST @snowpipe_ingest_db.ingest.gcp_stage;

// Create table first
CREATE OR REPLACE TABLE employees (
  id INT,
  first_name STRING,
  last_name STRING,
  email STRING,
  location STRING,
  department STRING
  );

//Grant access to user Role on Notification Integration
GRANT USAGE ON INTEGRATION gcp_notif_integration to ROLE ACCOUNTADMIN;

//Create Snowpipe Object
CREATE OR REPLACE PIPE employee_pipe
    AUTO_INGEST = TRUE
    INTEGRATION = 'GCP_NOTIF_INTEGRATION'
    AS
    COPY INTO snowpipe_ingest_db.ingest.employees
    FROM @snowpipe_ingest_db.ingest.gcp_stage ;

ALTER PIPE employee_pipe REFRESH;

//ALTER PIPE snowpipe_ingest_db.ingest.employee_pipe SET PIPE_EXECUTION_PAUSED= TRUE;
