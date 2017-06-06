-------------------------------------------------------------------------------
--- NUMBER OF TRAFFIC_DATA MSISDN_ID NOT IN M_LINES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of traffic_data with
---              msisdn_id field not in m_lines table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'traffic_data' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=traffic_data;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=5;

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
        td.*,
        IF ( mlines.msisdn_id IS NOT NULL, 'ok', 'ko' ) AS join_m_lines
    FROM (
        SELECT
            msisdn_id,
            month,
            gbic_op_id
        FROM gbic_dq_traffic_data
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS td
    LEFT OUTER JOIN (
        SELECT
            distinct msisdn_id,
            month,
            gbic_op_id
        FROM gbic_dq_m_lines_for_trafficdata
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS mlines
      ON  td.gbic_op_id = mlines.gbic_op_id
      AND td.month = mlines.month
      AND td.msisdn_id = mlines.msisdn_id
) z
GROUP BY join_m_lines;
