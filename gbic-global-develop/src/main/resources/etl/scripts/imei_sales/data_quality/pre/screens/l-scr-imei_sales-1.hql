-------------------------------------------------------------------------------
--- # RECORDS
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records in imei_sales file
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'imei_sales' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'L' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=imei_sales;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=L;
-- SET hivevar:screenCounter=1;

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
FROM gbic_dq_imei_sales
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}';
