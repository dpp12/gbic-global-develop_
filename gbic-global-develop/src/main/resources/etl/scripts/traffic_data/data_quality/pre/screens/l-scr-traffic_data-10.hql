----------------------------------------------------------------------------------
--- NUMBER OF TRAFFIC_DATA DUPLICATED PRIMARY KEYS
----------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of traffic_data with duplicated
---              primary key.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'traffic_data' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'L' in this case
---     screenCounter: Part of the screen Id. 10 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=traffic_data;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=L;
-- SET hivevar:screenCounter=10;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    ''                                         AS field,
    ''                                         AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        gbic_op_id,
        month,
        billing_cycle_id,
        subscription_id,
        msisdn_id,
        day_cd,
        time_range_cd,
        imei_num,
        billing_cycle_dt, 
        count(*) AS n 
    FROM gbic_dq_traffic_data
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
    GROUP BY 
        gbic_op_id,
        month,
        billing_cycle_id,
        subscription_id,
        msisdn_id,
        day_cd,
        time_range_cd,
        imei_num,
        billing_cycle_dt
    HAVING n>1
) a;
