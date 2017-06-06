-------------------------------------------------------------------------------
--- VALIDATE IF THERE ARE RECORDS WITH WRONG DAY_TYPE_CD FORMAT
-------------------------------------------------------------------------------
--- 
--- Description: Test if there are records of daily_traffic with day_type_cd
---              not in ('Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su')
--- Parameters:
--- 
---     gbic_op_id:  Country internal identification
---     fileName:    Name of the processed file. 'daily_traffic' in this case
---     nominalTime: Date of the file in format YYYY-mm-dd
---     test_id:     4 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=daily_traffic;
-- SET hivevar:nominalTime=2015-08-01;
-- SET hivevar:test_id=4;
SET hivevar:screenId=3;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_test_result
PARTITION ( gbic_op_id=${gbic_op_id}, file='${fileName}', day='${nominalTime}', test_id=${test_id} )
SELECT
    id_filerevision,
    id_test,
    test_type,
    ${screenId}         AS screen_number,
    test_number_file,
    test_field,
    test_field_content,
    test_expected_value AS test_expected_value,
    fieldMatchingValue  AS test_resulting_value,
    warn_threshold,
    error_threshold,
    CASE 
        WHEN ( test_expected_value - fieldMatchingValue = 0 ) THEN 'PASS'
        WHEN ( fieldTotalValue - fieldMatchingValue = 0) THEN 'ERROR'
        WHEN ( 100 * fieldMatchingValue/fieldTotalValue <= error_threshold ) THEN 'WARN'
        WHEN ( 100 * fieldMatchingValue/fieldTotalValue > error_threshold ) THEN 'ERROR'
        ELSE 'ERROR'
    END
FROM (
    SELECT
        id_filerevision,
        id_test,
        test_type,
        scResult.screenId,
        test_number_file,
        test_field,
        field_eval                                                                  AS test_field_content,
        test_expected_value,
        SUM( IF(fTest.field_eval = scResult.fieldContent, scResult.fieldValue, 0 )) AS fieldMatchingValue,
        SUM( fieldValue )                                                           AS fieldTotalValue,
        warn_threshold,
        error_threshold
    FROM (
        SELECT 
            *,
            test_field_content AS field_eval
        FROM gbic_dq_file_test
        WHERE gbic_op_id = ${gbic_op_id}
          AND file = '${fileName}'
          AND day = '${nominalTime}'
          AND test_type = 'G'
          AND test_number_file = ${test_id}
    ) fTest
    LEFT OUTER JOIN (
        SELECT
            substr(screenId,2) AS screenId,
            field,
            fieldContent,
            fieldValue
        FROM gbic_dq_screen_results
        WHERE gbic_op_id = ${gbic_op_id}
          AND file = '${fileName}'
          AND day = '${nominalTime}'
          AND screenid = 'G${screenId}'
    ) scResult
      ON fTest.test_field = scResult.field
    
    GROUP BY 
        id_filerevision,
        id_test,
        test_type,
        scResult.screenId,
        test_number_file,
        test_field,
        fTest.field_eval,
        test_expected_value,
        warn_threshold,
        error_threshold
) X;
