-------------------------------------------------------------------------------
--- # CUSTOMERS GROUPED BY SEGMENT_CD
-------------------------------------------------------------------------------
--- 
--- Description: Customers by segment_cd
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'customer' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'L' in this case
---     screenCounter: Part of the screen Id. 8 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=customer;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=L;
-- SET hivevar:screenCounter=8;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'segment_cd'                               AS field,
    segment_cd                                 AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM gbic_dq_customer
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
GROUP BY segment_cd;
