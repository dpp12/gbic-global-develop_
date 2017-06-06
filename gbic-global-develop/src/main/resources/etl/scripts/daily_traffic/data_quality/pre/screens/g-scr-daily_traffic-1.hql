-------------------------------------------------------------------------------
--- NUMBER OF DAILY_TRAFFIC NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of daily_traffic with one of its
---              values as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'daily_traffic' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=daily_traffic;
-- SET hivevar:nominalTime=2015-08-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=1;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'null_fields'                              AS field,
    null_fields                                AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT 
        *,
        IF ( country_id      IS NULL OR
             month_id        IS NULL OR
             call_dt         IS NULL OR
             msisdn_id       IS NULL OR
             imei_num        IS NULL OR
             day_type_cd     IS NULL OR
             bank_holiday_cd IS NULL OR
             roaming_cd      IS NULL OR
             calls_total_qt  IS NULL OR
             minutes_tot_qt  IS NULL OR
             sms_total_qt    IS NULL OR
             mb_total_qt     IS NULL OR
             subscription_id IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_daily_traffic
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
