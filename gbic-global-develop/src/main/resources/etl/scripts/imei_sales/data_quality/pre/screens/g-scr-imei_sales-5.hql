-------------------------------------------------------------------------------
--- NUMBER OF IMEI_SALES TARIFF_PLAN_ID NOT IN DIM_M_TARIFF_PLAN
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of imei_sales with tariff_plan_id
---              field not in dim_postal table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'imei_sales' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=imei_sales;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=5;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_m_tariff_plan'                   AS field,
    join_dim_m_tariff_plan                     AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        imei_sales.*,
        IF ( tariff.tariff_plan_id IS NOT NULL OR imei_sales.tariff_plan_id = '-1', 'ok', 'ko' ) AS join_dim_m_tariff_plan
    FROM (
        SELECT
            tariff_plan_id,
            month,
            gbic_op_id
        FROM gbic_dq_imei_sales
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS imei_sales
    LEFT OUTER JOIN (
        SELECT
            distinct tariff_plan_id,
            month,
            gbic_op_id
        FROM gbic_dq_dim_m_tariff_plan_for_mlines
    ) AS tariff
      ON imei_sales.tariff_plan_id = tariff.tariff_plan_id
) z
GROUP BY join_dim_m_tariff_plan;
