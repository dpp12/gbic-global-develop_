-------------------------------------------------------------------------------
--- RECORDS WITHOUT ACCESS
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any customers
---
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_tariff_plan' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 2 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_tariff_plan;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=2;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_f_tariff_plan'                       AS field,
    join_f_tariff_plan                         AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        lines.*,
        IF (f_tariff.subscription_id IS NOT NULL, 'ok', 'ko') AS join_f_tariff_plan
    FROM (
        SELECT subscription_id,
               month,
               gbic_op_id
        FROM gbic_dq_f_tariff_plan
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS lines
    LEFT OUTER JOIN
    (
        SELECT distinct subscription_id,
               month,
               gbic_op_id
        FROM gbic_dq_f_lines_for_ftariffplan
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
          AND voice_ind>0
    ) AS f_tariff
      ON  lines.gbic_op_id = f_tariff.gbic_op_id
      AND lines.subscription_id = f_tariff.subscription_id
) AS customers
GROUP BY join_f_tariff_plan;
