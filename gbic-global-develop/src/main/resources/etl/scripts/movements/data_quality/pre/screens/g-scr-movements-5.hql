-------------------------------------------------------------------------------
--- NUMBER OF MOVEMENTS SEGMENT_CD NOT IN SEGMENTS TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with segment_cd field
---              not in segment table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=5;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_segments'                            AS field,
    join_segments                              AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT movements.*,
    IF ( segments.seg_local_cd IS NOT NULL, 'ok', 'ko' ) AS join_segments
    FROM (
        SELECT
            upper(segment_cd) AS segment,
            month,
            gbic_op_id
        FROM gbic_dq_movements
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS movements
    LEFT OUTER JOIN (
        SELECT
            distinct seg_local_cd,
            month,
            seg_gbic_op_id
        FROM gbic_dq_segments_for_movements
        WHERE seg_gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS segments
    ON movements.segment = segments.seg_local_cd
      AND movements.month = segments.month
      AND movements.gbic_op_id = segments.seg_gbic_op_id
)z 
GROUP BY join_segments;
