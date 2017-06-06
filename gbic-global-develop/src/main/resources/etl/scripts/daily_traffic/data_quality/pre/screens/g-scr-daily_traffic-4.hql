-------------------------------------------------------------------------------
--- NUMBER OF WRONG FORMAT OF CALL_DT
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of daily_traffic with wrong format
---              for call_dt
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'daily_traffic' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 4 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=daily_traffic;
-- SET hivevar:nominalTime=2015-08-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=4;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'call_dt_ok'                               AS field,
    call_dt_ok                                 AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        *,
        IF ( call_dt not rlike '[0-9]{4}-[0-9]{2}-[0-9]{2}', 'ko', 'ok' ) AS call_dt_ok
    FROM gbic_dq_daily_traffic
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file

GROUP BY call_dt_ok;
