-------------------------------------------------------------------------------
--- NUMBER OF WRONG FORMAT OF BIRTH_DT
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of consumer segment with
---              party_identification_num field whose length is not the
---              expected according to encryption applied by the country
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'customer' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 3 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=customer;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=3;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'birth_dt_ok'                              AS field,
    birth_dt_ok                                AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        *,
        IF ( birth_dt not rlike '[0-9]{4}-[0-9]{2}-[0-9]{2}', 'ko', 'ok' ) AS birth_dt_ok
    FROM gbic_dq_customer
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file

GROUP BY birth_dt_ok;
