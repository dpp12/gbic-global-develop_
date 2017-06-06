--- @deprecated services_line is deprecated. Use m_line_services instead.
-------------------------------------------------------------------------------
--- NUMBER OF SERVICES_LINE ID_SERVICE NOT IN DIM_M_SERVICES TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of services_line with id_service
---              field not in dim_m_services table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'services_line' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 6 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=services_line;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=6;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_m_services'                      AS field,
    join_dim_m_services                        AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        serv.*,
        IF ( dim_serv.id_service IS NOT NULL OR serv.id_service = '-1', 'ok', 'ko' ) AS join_dim_m_services
    FROM (
        SELECT id_service
        FROM gbic_dq_services_line
        WHERE gbic_op_id = ${gbic_op_id} 
          AND month = '${nominalTime}'
    ) AS serv
    LEFT OUTER JOIN (
        SELECT distinct id_service
        FROM gbic_dq_dim_m_services_for_servicesline
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS dim_serv
    ON serv.id_service = dim_serv.id_service
)z 
GROUP BY join_dim_m_services;
