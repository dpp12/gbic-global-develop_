-------------------------------------------------------------------------------
--- # DUPLICATED PRIMARY KEYS (COUNTRY_ID, MONTH_ID, ID_SERVICE)
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of pk duplicates in dim_m_services file
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'dim_m_services' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'L' in this case
---     screenCounter: Part of the screen Id. 2 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=dim_m_services;
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
        country_id,
        month_id,
        id_service,
        COUNT(*) AS n
    FROM gbic_dq_dim_m_services
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
    GROUP BY
        country_id,
        month_id,
        id_service
    HAVING n > 1
) x;
