-------------------------------------------------------------------------------
--- VALIDATE SERVICE/LOCAL QUALITY MARKERS IN GBIC
-------------------------------------------------------------------------------
--- 
--- Description: Matches quality data received from the service with the
---              info get by gbic on data files
--- Parameters:
--- 
---     gbic_op_id:  Country internal identification
---     fileName:    Name of the processed file. 'dim_m_services' in this case
---     nominalTime: Date of the file in format YYYY-mm-dd
---     test_id:     1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=dim_m_services;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:test_id=1;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_test_result
PARTITION ( gbic_op_id=${gbic_op_id}, file='${fileName}', day='${nominalTime}', test_id=${test_id} )
SELECT
    id_filerevision,
    id_test             AS idTest,
    test_type,
    IF ( scBase.screenId IS NULL, -1, scBase.screenId)         AS screen_number,
    test_number_file,
    IF ( scBase.field IS NULL, '', scBase.field)               AS test_field,
    IF ( scBase.fieldContent IS NULL, '', scBase.fieldContent) AS test_field_content,
    IF ( scBase.fieldValue IS NULL, -1, scBase.fieldValue)     AS test_expected_value,
    IF ( scResult.fieldValue IS NULL, -1, scResult.fieldValue) AS test_resulting_value,
    warn_threshold,
    error_threshold,
    CASE 
        WHEN ( scBase.fieldValue - scResult.FieldValue = 0 ) THEN 'PASS'
        WHEN ( 100 * abs(scBase.fieldValue - scResult.FieldValue)/scBase.fieldValue <= error_threshold ) THEN 'WARN'
        WHEN ( 100 * abs(scBase.fieldValue - scResult.FieldValue)/scBase.fieldValue > error_threshold ) THEN 'ERROR'
        ELSE 'ERROR'
    END
FROM (
    SELECT
        screenNumber                                AS screenId,
        IF (field IS NULL, '', field)               AS field,
        IF (fieldContent IS NULL, '', fieldContent) AS fieldContent,
        fieldValue
    FROM gbic_dq_service_checks
    WHERE gbic_op_id = ${gbic_op_id}
      AND file = '${fileName}'
      AND day = '${nominalTime}'
) scBase
LEFT OUTER JOIN (
    SELECT
        substr(screenId,2)                           AS screenId,
        IF ( field IS NULL, '', field)               AS field,
        IF ( fieldContent IS NULL, '', fieldContent) AS fieldContent,
        fieldValue
    FROM gbic_dq_screen_results
    WHERE gbic_op_id = ${gbic_op_id}
      AND file = '${fileName}'
      AND day = '${nominalTime}'
      AND screenid like 'L%'
) scResult
  ON  scBase.screenId = scResult.screenId
  AND lower(scBase.field) = scResult.field
  AND scBase.fieldContent = scResult.fieldContent

FULL OUTER JOIN (
    SELECT
        *
    FROM gbic_dq_file_test
    WHERE gbic_op_id = ${gbic_op_id}
      AND file = '${fileName}'
      AND day = '${nominalTime}'
      AND test_type = 'L'
) fTest;
