-------------------------------------------------------------------------------
--- RECORDS WITHOUT BBAND_TARIFF_PLAN_ID
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any
---              bband_tariff_plan_id
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=5;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_f_tariff_plan_bband'             AS field,
    join_dim_f_tariff_plan_bband               AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        lines.*,
        IF (tariff.f_tariff_plan_id IS NOT NULL OR lines.bband_tariff_plan_id ='-1', 'ok', 'ko') AS join_dim_f_tariff_plan_bband
    FROM (
        SELECT bband_tariff_plan_id,
               month,
               gbic_op_id
        FROM gbic_dq_f_lines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS lines
    LEFT OUTER JOIN
    (
        SELECT distinct f_tariff_plan_id,
               month,
               gbic_op_id
        FROM gbic_dq_dim_f_tariff_plan_for_flines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS tariff
      ON  lines.gbic_op_id = tariff.gbic_op_id
      AND lines.bband_tariff_plan_id = tariff.f_tariff_plan_id
) AS tariffs
GROUP BY join_dim_f_tariff_plan_bband;
