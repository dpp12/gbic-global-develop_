-------------------------------------------------------------------------------
--- NUMBER OF DIM_POSTAL LOCATION_LEVEL NOT IN HOMOGENIZATION FILES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of dim_postal with
---              location_level field not in homogenization files
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'dim_postal' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 2 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=dim_postal;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=2;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_homog_files'                         AS field,
    join_homog_files                           AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        dim_postal.*,
        IF (location.local_cd IS NOT NULL, 'ok', 'ko') AS join_homog_files
    FROM (
        SELECT
            location_level,
            month,
            gbic_op_id
        FROM gbic_dq_dim_postal
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS dim_postal
    LEFT OUTER JOIN (
        SELECT
            local_cd,
            month,
            gbic_op_id
        FROM gbic_dq_location_for_dimpostal
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS location
      ON location.local_cd = dim_postal.location_level
        AND location.gbic_op_id = dim_postal.gbic_op_id
) z
GROUP BY join_homog_files;
