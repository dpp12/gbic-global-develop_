--- @deprecated services_line is deprecated. Use m_line_services instead.
-------------------------------------------------------------------------------
--- NUMBER OF SERVICES_LINE NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of services_line with
---              one of its values as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'services_line' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=services_line;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=1;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'null_fields'                              AS field,
    null_fields                                AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT 
        *,
        IF ( country_id       IS NULL OR
             month_id         IS NULL OR
             msisdn_id        IS NULL OR
             subscription_id  IS NULL OR
             activation_dt    IS NULL OR
             id_service       IS NULL OR
             group_sva        IS NULL OR
             service_activ_dt IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_services_line
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
