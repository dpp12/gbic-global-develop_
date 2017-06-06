-------------------------------------------------------------------------------
--- RECORDS WITHOUT DIM_POSTAL
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any homogenization
---              dim_postal
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 3 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=3;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_postal'                          AS field,
    join_dim_postal                            AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        lines.*,
        IF (dimp.postal_id IS NOT NULL OR lines.postal_cd ='-1', 'ok', 'ko') AS join_dim_postal
    FROM (
        SELECT postal_cd,
               month,
               gbic_op_id
        FROM gbic_dq_f_lines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS lines
    LEFT OUTER JOIN
    (
        SELECT distinct postal_id,
               month,
               gbic_op_id
        FROM gbic_dq_dim_postal_for_flines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS dimp
      ON  lines.gbic_op_id = dimp.gbic_op_id
      AND lines.postal_cd = dimp.postal_id
) AS dim_postal
GROUP BY join_dim_postal;
