--- @deprecated services_line is deprecated. Use m_line_services instead.
-------------------------------------------------------------------------------
--- # DUPLICATED PRIMARY KEYS (COUNTRY_ID, MONTH_ID, SUBSCRIPTION_ID, MSISDN_ID,
---                            ID_SERVICE, SERVICE_ACTIV_DATE)
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of pk duplicates in services_line's file
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'services_line' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 9 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=services_line;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=9;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'count_pk'                                 AS field,
    count_pk                                   AS fieldContent,
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
        IF ( count(*) > 1, 'ko', 'ok' ) AS count_pk
    FROM gbic_dq_services_line
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
    GROUP BY
        gbic_op_id,
        month,
        subscription_id,
        msisdn_id,
        id_service,
        service_activ_dt
) x
GROUP BY count_pk;
