-------------------------------------------------------------------------------
--- NUMBER OF M_LINE_SERVICES GROUP_SVA NOT IN DIM_M_GROUP_SVA TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of m_line_services with group_sva
---              field not in dim_m_group_sva table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_line_services' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 7 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_line_services;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=7;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_m_group_sva'                     AS field,
    join_dim_m_group_sva                       AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        serv.*,
        IF ( dim_group.group_sva IS NOT NULL OR serv.group_sva = '-1', 'ok', 'ko' ) AS join_dim_m_group_sva
    FROM (
        SELECT group_sva
        FROM gbic_dq_m_line_services
        WHERE gbic_op_id = ${gbic_op_id} 
          AND month = '${nominalTime}'
    ) AS serv
    LEFT OUTER JOIN (
        SELECT distinct group_sva
        FROM gbic_dq_dim_m_group_sva_for_mlineservices
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS dim_group
    ON serv.group_sva = dim_group.group_sva
)z 
GROUP BY join_dim_m_group_sva;
