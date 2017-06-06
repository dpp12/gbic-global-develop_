-------------------------------------------------------------------------------
--- NUMBER OF MOVEMENTS MOVEMENT_ID NOT IN DIM_M_MOVEMENT TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with movement_id field
---              not in dim_m_movements table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 6 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=6;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_m_movement'                      AS field,
    join_dim_m_movement                        AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT movements.*,
    IF ( dim_m_movements.movement_id IS NOT NULL OR movements.movement_id ='-1', 'ok', 'ko' ) AS join_dim_m_movement
    FROM (
        SELECT
            distinct movement_id,
            month,
            gbic_op_id
        FROM gbic_dq_movements
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS movements
    LEFT OUTER JOIN (
        SELECT
            movement_id,
            month,
            gbic_op_id
        FROM gbic_dq_dim_m_movement_for_movements
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS dim_m_movements
    ON movements.movement_id = dim_m_movements.movement_id
      AND movements.month = dim_m_movements.month
      AND movements.gbic_op_id = dim_m_movements.gbic_op_id
)z 
GROUP BY join_dim_m_movement;
