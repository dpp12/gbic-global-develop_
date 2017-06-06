-------------------------------------------------------------------------------
--- NUMBER OF M_LINE_SERVICES WITH WRONG SERVICE_ACTIV_DT FORMAT
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of m_line_services with wrong
---              service_activ_dt format
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_line_services' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 4 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_line_services;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=4;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'service_active_dt_format'                 AS field,
    service_active_dt_format                   AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT 
        *,
        IF ( service_activ_dt NOT RLIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]', 'ko', 'ok' ) AS service_active_dt_format
    FROM gbic_dq_m_line_services
    WHERE gbic_op_id = ${gbic_op_id}
      AND month ='${nominalTime}'
) file
GROUP BY service_active_dt_format;
