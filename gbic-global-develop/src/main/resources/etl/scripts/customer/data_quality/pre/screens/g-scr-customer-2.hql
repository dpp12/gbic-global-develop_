-------------------------------------------------------------------------------
--- RECORDS WITHOUT SEGMENT
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any homogenization
---              segment or labeled as Others
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'customer' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 2 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=customer;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=2;

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
    SELECT
        customer.*,
        IF (seg.seg_local_cd IS NOT NULL, 'ok', 'ko') AS join_segments
    FROM (
        SELECT
            upper(segment_cd) AS segment,
            month,
            gbic_op_id
        FROM gbic_dq_customer
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS customer
    LEFT OUTER JOIN (
        SELECT
            seg_local_cd,
            month,
            seg_gbic_op_id
        FROM gbic_dq_segments_for_customer
        WHERE month = '${nominalTime}'
          AND seg_gbic_op_id = ${gbic_op_id}
    ) AS seg
      ON customer.segment = seg.seg_local_cd
      AND customer.gbic_op_id = seg.seg_gbic_op_id
) z
GROUP BY join_segments;
