-------------------------------------------------------------------------------
--- NUMBER OF RECORDS WITH WRONG DAY_TYPE_CD FORMAT 
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of daily_traffic with day_type_cd
---              not in ('Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su')
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'daily_traffic' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 3 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=daily_traffic;
-- SET hivevar:nominalTime=2015-08-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=3;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'day_type_format'                          AS field,
    day_type_format                            AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        *,
        IF ( day_type_cd IN ('Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'), 'ok', 'ko' ) AS day_type_format
    FROM gbic_dq_daily_traffic
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file

GROUP BY day_type_format;
