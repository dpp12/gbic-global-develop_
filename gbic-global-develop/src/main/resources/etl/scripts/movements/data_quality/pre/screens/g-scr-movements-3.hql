-------------------------------------------------------------------------------
--- NUMBER OF MOVEMENTS SUBSCRIPTION_ID NOT ANONYMIZED
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with subscription_id
---              field whose length is not the expected according to encryption
---              applied by the country
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 3 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=3;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'subscription_id_length'                   AS field,
    length(subscription_id)                    AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM gbic_dq_movements
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
GROUP BY length (subscription_id);
