USE DATABASE dev_db;
USE SCHEMA dev_db.snowpark_lab;

CREATE OR REPLACE PROCEDURE dev_db.snowpark_lab.LOAD_ROLLING_6W()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import logging
import uuid

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.types import StructType, StructField, LongType, DoubleType, StringType
from snowflake.snowpark.functions import col

PIPELINE   = 'ROLLING_6W_SNOWPARK'
KEEP_WEEKS = 6
STAGING_TABLE = 'DEV_DB.SNOWPARK_LAB.STAGED_SALES_TABLE'
TARGET_TABLE = 'DEV_DB.SNOWPARK_LAB.SALES_ROLLING'
STAGING_OBJECT = '@DEV_DB.SNOWPARK_LAB.GCS_CSV_EXT_STAGE'
LOG_TABLE = "DEV_DB.SNOWPARK_LAB.PIPELINE_LOG"

FILE_SCHEMA = StructType([
    StructField("ORDER_ID",         LongType()),
    StructField("PRODUCT",          StringType()),
    StructField("QTY",              LongType()),
    StructField("PRICE",            DoubleType()),
    StructField("FISCALWEEK",       LongType()),
    StructField("ORDER_FISCALWEEK", StringType()),
])


logger = logging.getLogger(PIPELINE)


def _fiscal_week(session, weeks_back=0):
    #Function to Calculate the Current and Previous 6th week
    n = int(weeks_back)

    query = (
        f"SELECT YEAROFWEEKISO(DATEADD(week, -{n}, CURRENT_DATE())) * 100 +"
        F" WEEKISO(DATEADD(week, -{n}, CURRENT_DATE()))"
    )
    
    result = session.sql(query).collect()[0][0]
    return result

    
def _upload_stage(session : Session, file_path : str, current_fw):
    #Function to Upload the Staging Table from the External Stage
    logger.info("Reading data from stage: %s", file_path)

    file_name = f"orders_FW{current_fw}.csv"
    full_path = f"{file_path.rstrip('/')}/{file_name}"   
        
    df = (session.read
         .schema(FILE_SCHEMA)
         .option("TYPE", "CSV")
         .option("skip_header", 1)
         .option("field_optionally_enclosed_by", '"')
         .csv(full_path))

    logger.info("Writing data to transient table: %s", STAGING_TABLE)
        
        
    df.write.mode("overwrite").save_as_table(STAGING_TABLE, table_type="transient")

    stg = session.table(STAGING_TABLE)
    return stg
        
        
def _validate_data(stg, n, current_fw):
    #Function to Validate the data in the Staging Table 
    
    flag = True    
    curr_fw = stg.select("FISCALWEEK").distinct().collect()[0]["FISCALWEEK"]
    logger.info("Data is for the week: %s", curr_fw)
    
    if curr_fw == current_fw:
        
        logger.info("Data is for the curret week. Will do the next steps of validation")

        null_rows = stg.filter(col("ORDER_ID").is_null() | col("PRODUCT").is_null()
                             | col("QTY").is_null() | col("PRICE").is_null() | col("FISCALWEEK").is_null() 
                             | col("ORDER_FISCALWEEK").is_null()).count()

        if null_rows:
            raise ValueError(f"{null_rows} rows have null/invalid values")
            flag = False
            

        dups = n - stg.select("ORDER_FISCALWEEK").distinct().count()

        if dups:
            raise ValueError(f"{dups} duplicate ORDER_FISCALWEEK values in file")
            flag = False
            


    else: 
        raise ValueError(f"The file does not contain the current week %s data", current_fw)
        logger.info("Data in the file is not of the current week: %s", current_fw)
        flag = False
        

    return flag
        
def _upload_target_table(session: Session,  current_fw): 
    #Function to insert the data from the Staging table to the TARGET Table

    file_name = f"orders_FW{current_fw}.csv"
    #full_path = f"{file_path.rstrip('/')}/{file_name}"   

    
    upload_query = (
                f"INSERT INTO {TARGET_TABLE}" 
                f"(ORDER_ID, PRODUCT, QTY, PRICE, FISCAL_WEEK, ORDER_FISCALWEEK, SOURCE_FILE, LOAD_TS)"
                f"SELECT ORDER_ID, PRODUCT, QTY, PRICE, FISCALWEEK, ORDER_FISCALWEEK, '{file_name}', CURRENT_TIMESTAMP() "
                f"FROM {STAGING_TABLE}"
    )

    upload_result = session.sql(upload_query).collect()[0][0]
    logger.info("Data uploaded to the Target Table successfully")
    logger.info("Total rows uploaded to the target table is: %s", upload_result)
    return upload_result


def _delete_7week_data(session: Session, fiscal_week):
    #Function to Delete the data from Target Table for fiscal_week
    del_query = (
        f"DELETE FROM {TARGET_TABLE} WHERE FISCAL_WEEK = {fiscal_week}"
    )

    del_results = session.sql(del_query).collect()[0][0]
    
    if del_results > 0:
        logger.info("Data deleted from the TARGET_TABLE table successfully")
    else:
        logger.info("No records found for the fiscal_week in the TARGET_TABLEE")
        
    return del_results

def post_validation(session: Session):
    # Function for Post Validation
    validation_query = (
            f"SELECT COUNT(DISTINCT FISCAL_WEEK), COUNT(*) FROM {TARGET_TABLE}"
            )

    weeks, total = session.sql(validation_query).collect()[0]
    return weeks, total
        
def _flush(session, run_id, events):
    #Function written after COMMIT/ROLLBACK so a rollback never erases the audit trail.
    for step, status, rows, msg in events:
        try:
            session.sql(
                f"INSERT INTO {LOG_TABLE} (RUN_ID, PIPELINE, STEP, STATUS, ROWS_AFFECTED, MESSAGE) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                params=[run_id, PIPELINE, step, status, rows, (msg or "")[:4000]],
            ).collect()
            
        except Exception as e:  # logging must never mask the real outcome
            logger.warning("Could not write log row: %s", e)



def run(session: Session) -> str:

    run_id = uuid.uuid4().hex[:12]
    events = []

    def ev(level, step, status, rows=None, msg=""):
        getattr(logger, level)("run=%s [%s] %s rows=%s %s", run_id, step, status, rows, msg)
        events.append((step, status, rows, msg))

        
    stg_df = None
    row_count = 0
    in_txn = False
    
    try:
        # ---- 1. Indentify the Current week and Previous 6th week
        
        current_fw = _fiscal_week(session, 0)               # 202641
        prev_7_fw  = _fiscal_week(session, KEEP_WEEKS )  # 202634
        print(f"current_week={current_fw} prev_7_week={prev_7_fw}")
        ev("info", "START", "INFO", None, f"file={STAGING_OBJECT} Current_Week={current_fw} Previous_week={prev_7_fw}")
    except Exception as exc:
        logger.error("Fiscal week calculation failed: %s", exc)
        raise        # re-raise so the real error appears in the CALL result
        _flush(session, run_id, events)


        
    try:
        # ---- 2. Read DEV_DB.PUBLIC.STAGED_SALES_TABLE file from stage into the staging table (outside the trnx) and Validate it.
        
        stg_df = _upload_stage(session, STAGING_OBJECT, current_fw)
        if stg_df is not None:
            row_count = stg_df.count()
            logger.info("Staging complete. Total rows loaded: %s", row_count)
            flag = _validate_data(stg_df, row_count, current_fw)
            ev("info", "STAGE_AND_VALIDATE", "OK", row_count, f"staged to {STAGING_TABLE}; schema, nulls, duplicates passed")

    except Exception as exc:
        logger.error("Error in uploading the Staging data: %s", exc)
        raise
        _flush(session, run_id, events)
    
    if flag:
        """ ---- 3. Delete DEV_DB.PUBLIC.SALES_ROLLING table for current week, Upload data and Delete data for previous 6th week """
        try:
            in_txn = True
            
            session.sql("BEGIN").collect()

            
            replaced = _delete_7week_data(session, current_fw)
            logger.info("Delete Current Week data from TARGET_TABLE. Total rows Deleted: %s", replaced)
            
            inserted = _upload_target_table(session, current_fw)
            logger.info("Target Upload is complete. Total rows loaded: %s", inserted)
            
            purged = _delete_7week_data(session, prev_7_fw)
            logger.info("Final Delete of Previous 7th week is complete. Total rows Deleted: %s", purged)

            session.sql("COMMIT").collect()

            in_txn = False

            ev("info", "REPLACE_SAME_WEEK", "OK", replaced, f"Current_Week={current_fw}")
            ev("info", "INSERT", "OK", inserted, f"Current_Week={current_fw}")
            ev("info", "PURGE", "OK", purged, f"deleted Previous Week = {prev_7_fw}")

            weeks, total = post_validation(session)
            
            if weeks > KEEP_WEEKS:
                ev("warning", "POST_CHECK", "WARN", total, f"{weeks} weeks present, expected <= {KEEP_WEEKS}")
            else:
                ev("info", "POST_CHECK", "OK", total, f"{weeks} week(s) in table")
                
            ev("info", "FINISH", "OK", inserted, "run completed successfully")
        
            return (f"OK run_id={run_id} Current_Week={current_fw} replaced={replaced} "
                    f"inserted={inserted} purged={purged} weeks_in_table={weeks}")

        except Exception as exc:
            if in_txn:
                try:
                    session.sql("ROLLBACK").collect()
                    logger.error("run=%s transaction rolled back; target unchanged", run_id)
                except Exception:
                    logger.exception("ROLLBACK failed")
                ev("error", "FAILED", "ERROR", None, f"{type(exc).__name__}: {exc}")
                raise   # re-raise so the Task run shows FAILED and alerts can fire

        finally:
            _flush(session, run_id, events)
 
$$;

//CALL dev_db.snowpark_lab.LOAD_ROLLING_6W();

//SHOW PROCEDURES LIKE 'LOAD_ROLLING_6W' IN ACCOUNT;

CREATE OR REPLACE TASK ROLLING_6W_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 30 13 * * MON Asia/Kolkata'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3          -- stop retrying a broken pipeline
AS
    CALL LOAD_ROLLING_6W('@weekly_stage/sales_latest.csv', NULL);

-- Tasks are created suspended
ALTER TASK ROLLING_6W_TASK RESUME;

ALTER TASK ROLLING_6W_TASK SUSPEND;
