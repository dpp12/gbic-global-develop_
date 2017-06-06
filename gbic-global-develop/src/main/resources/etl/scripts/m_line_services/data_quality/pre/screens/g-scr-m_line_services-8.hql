-------------------------------------------------------------------------------
--- NUMBER OF M_LINE_SERVICES SUBSCRIPTION_ID NOT IN M_LINES TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of m_line_services with
---              subscription_id field not in dim_m_group_sva table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_line_services' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 8 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_line_services;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=8;

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
        serv.*,
        IF ( mlines.subscription_id IS NOT NULL AND mlines.msisdn_id IS NOT NULL, 'ok', 'ko' ) AS join_m_lines
    FROM (
        SELECT
            subscription_id, 
            msisdn_id, 
            month, 
            gbic_op_id
        FROM gbic_dq_m_line_services
        WHERE gbic_op_id = ${gbic_op_id} 
          AND month = '${nominalTime}'
    ) AS serv
    LEFT OUTER JOIN (
        SELECT
            subscription_id,
            msisdn_id,
            month,
            gbic_op_id
        FROM gbic_dq_m_lines_for_mlineservices
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS mlines
    ON serv.gbic_op_id = mlines.gbic_op_id
      AND serv.subscription_id = mlines.subscription_id
      AND serv.msisdn_id = mlines.msisdn_id
)z 
GROUP BY join_m_lines;
