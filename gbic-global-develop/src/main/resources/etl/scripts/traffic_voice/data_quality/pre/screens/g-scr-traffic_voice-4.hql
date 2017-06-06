-------------------------------------------------------------------------------
--- NUMBER OF TRAFFIC_VOICE MSISDN_ID NOT IN M_LINES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of traffic_voice with
---              msisdn_id field not in m_lines table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'traffic_voice' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 4 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=traffic_voice;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=4;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_m_lines'                             AS field,
    join_m_lines                               AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        tv.*,
        IF ( mlines.msisdn_id IS NOT NULL, 'ok', 'ko' ) AS join_m_lines
    FROM (
        SELECT
            msisdn_id,
            month,
            gbic_op_id
        FROM gbic_dq_traffic_voice
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS tv
    LEFT OUTER JOIN (
        SELECT
            distinct msisdn_id,
            month,
            gbic_op_id
        FROM gbic_dq_m_lines_for_trafficvoice
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS mlines
      ON  tv.gbic_op_id = mlines.gbic_op_id
      AND tv.msisdn_id = mlines.msisdn_id
) z
GROUP BY join_m_lines;
