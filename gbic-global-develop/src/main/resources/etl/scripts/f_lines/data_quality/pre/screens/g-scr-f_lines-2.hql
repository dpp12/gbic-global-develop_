-------------------------------------------------------------------------------
--- RECORDS WITHOUT SEGMENT_CD
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any homogenization
---              segment_cd
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 8 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=2;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_segment_cd'                          AS field,
    join_segment_cd                            AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        lines.*,
        IF (seg.seg_local_cd IS NOT NULL, 'ok', 'ko') AS join_segment_cd
    FROM (
        SELECT upper(segment_cd) AS segment,
               month,
               gbic_op_id
        FROM gbic_dq_f_lines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS lines
    LEFT OUTER JOIN 
    (
        SELECT seg_local_cd,
               month,
               seg_gbic_op_id
        FROM gbic_dq_segments_for_flines
        WHERE seg_gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS seg
      ON  lines.gbic_op_id = seg.seg_gbic_op_id
      AND lines.segment = seg.seg_local_cd
) AS segments
GROUP BY join_segment_cd;
