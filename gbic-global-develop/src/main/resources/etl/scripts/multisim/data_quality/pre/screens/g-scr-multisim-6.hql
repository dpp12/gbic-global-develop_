-------------------------------------------------------------------------------
--- # DUPLICATED PRIMARY KEYS (COUNTRY_ID, MONTH_ID, MSISDN_MAIN, MSISDN_ADD)
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of pk duplicates in multisim's file
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'multisim' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 6 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=multisim;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=6;

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
        country_id,
        month_id,
        msisdn_main,
        msisdn_add,
        IF ( count(*) > 1, 'ko', 'ok' ) AS count_pk
    FROM gbic_dq_multisim
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
    GROUP BY
        country_id,
        month_id,
        msisdn_main,
        msisdn_add
) x
GROUP BY count_pk;
