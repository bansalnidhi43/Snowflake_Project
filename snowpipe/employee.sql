//Create Database
USE DATABASE snowpipe_ingest_db;


//Create Schema 
CREATE SCHEMA IF NOT EXISTS ingest;


//Create Storage Integration
CREATE STORAGE INTEGRATION IF NOT EXISTS gcp_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake_bckt02/csv/snowpipe/');

DESC STORAGE INTEGRATION gcp_integration;

//Create Notification Integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS gcp_notif_integration
    ENABLED = TRUE
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = GCP_PUBSUB
    GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/snowflake-project-510103/subscriptions/snowflake_topic02-sub';

DESC NOTIFICATION INTEGRATION gcp_notif_integration;


// Create file format object
CREATE file format IF NOT EXISTS csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;


//Create External Stage Object
CREATE STAGE IF NOT EXISTS gcp_stage
    URL = 'gcs://snowflake_bckt02/csv/snowpipe/'
    STORAGE_INTEGRATION = gcp_integration
    FILE_FORMAT = snowpipe_ingest_db.ingest.csv_fileformat
    directory = (enable = True);

LIST @snowpipe_ingest_db.ingest.gcp_stage;

// Create table first
CREATE TABLE IF NOT EXISTS employees (
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
CREATE PIPE IF NOT EXISTS employee_pipe
    AUTO_INGEST = TRUE
    INTEGRATION = 'GCP_NOTIF_INTEGRATION'
    AS
    COPY INTO snowpipe_ingest_db.ingest.employees
    FROM @snowpipe_ingest_db.ingest.gcp_stage 
    ON_ERROR = SKIP_FILE;

ALTER PIPE employee_pipe REFRESH;

//ALTER PIPE snowpipe_ingest_db.ingest.employee_pipe SET PIPE_EXECUTION_PAUSED= TRUE;

//Create EMAIL Notification Integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS my_email_int
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('bansal.nidhi43@gmail.com');

//To test if the Email Integration is working fine
//CALL SYSTEM$SEND_EMAIL('my_email_int', 'bansal.nidhi43@gmail.com', 'Test', 'Hello from Snowflake');


//Create the Automatic Alert on Load Failure or Partial Load
CREATE ALERT IF NOT EXISTS pipe_failure_alert
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTES'
 IF (EXISTS (
    SELECT 1
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
           TABLE_NAME => 'OUR_FIRST_DB.PUBLIC.EMPLOYEES',
           START_TIME => DATEADD(hour, -2, CURRENT_TIMESTAMP())))
    WHERE STATUS IN ('Load failed','Partially loaded')
      AND LAST_LOAD_TIME > SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME()
  ))
THEN
CALL SYSTEM$SEND_EMAIL(
    'my_email_int',
    'bansal.nidhi43@gmail.com',
    'Snowpipe load failure',
    'A file was skipped or partially loaded. Check COPY_HISTORY.'
);

ALTER ALERT pipe_failure_alert RESUME; //Alert is created in SUSPEND Mode
//ALTER ALERT pipe_failure_alert SUSPEND; 


        
