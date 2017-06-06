-------------------------------------------------------------------------------
--- NUMBER OF MOVEMENTS TARIFF_PLAN_ID NOT IN DIM_M_TARIFF_PLAN TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with tariff_plan_id
---              field not in dim_m_tariff_plan table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 7 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=7;

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
    SELECT movements.*,
    IF ( dim_m_movements.tariff_plan_id IS NOT NULL OR movements.tariff_plan_id ='-1', 'ok', 'ko' ) AS join_dim_m_tariff_plan
    FROM (
        SELECT
            tariff_plan_id,
            month,
            gbic_op_id
        FROM gbic_dq_movements
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS movements
    LEFT OUTER JOIN (
        SELECT
            distinct tariff_plan_id,
            month,
            gbic_op_id
        FROM gbic_dq_dim_m_tariff_plan_for_movements
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS dim_m_movements
    ON movements.tariff_plan_id = dim_m_movements.tariff_plan_id
      AND movements.month = dim_m_movements.month
      AND movements.gbic_op_id = dim_m_movements.gbic_op_id
)z 
GROUP BY join_dim_m_tariff_plan;
