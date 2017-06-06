-------------------------------------------------------------------------------
--- # DUPLICATED PRIMARY KEYS
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of m_line_services with duplicated
---              primary key
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_line_services' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'L' in this case
---     screenCounter: Part of the screen Id. 2 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_line_services;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=L;
-- SET hivevar:screenCounter=2;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    ''                                         AS field,
    ''                                         AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        gbic_op_id,
        month,
        subscription_id,
        msisdn_id,
        id_service,
        service_activ_dt,
        COUNT(*) AS n 
    FROM gbic_dq_m_line_services
    WHERE gbic_op_id = ${gbic_op_id}
      AND month='${nominalTime}'
    GROUP BY
        gbic_op_id,
        month,
        subscription_id,
        msisdn_id,
        id_service,
        service_activ_dt
    HAVING n>1
) a;
