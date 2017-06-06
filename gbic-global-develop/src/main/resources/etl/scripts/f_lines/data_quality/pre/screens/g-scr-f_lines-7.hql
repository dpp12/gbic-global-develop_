-------------------------------------------------------------------------------
--- RECORDS WITHOUT CUSTOMERS
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any customers
---
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 6 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=7;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_customers'                           AS field,
    join_customers                             AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        lines.*,
        IF (custs.customer_id IS NOT NULL, 'ok', 'ko') AS join_customers
    FROM (
        SELECT customer_id,
               month,
               gbic_op_id
        FROM gbic_dq_f_lines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
          AND (
                  (voice_ind != 0 AND
                   bband_ind != 0 AND
                   tv_ind    != 0 AND
                   gbic_op_id = 3)
               OR
                   (gbic_op_id != 3)
              )
    ) AS lines
    LEFT OUTER JOIN
    (
        SELECT distinct customer_id,
               month,
               gbic_op_id
        FROM gbic_dq_customer_for_flines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS custs
      ON  lines.gbic_op_id = custs.gbic_op_id
      AND lines.customer_id = custs.customer_id
) AS customers
GROUP BY join_customers;
