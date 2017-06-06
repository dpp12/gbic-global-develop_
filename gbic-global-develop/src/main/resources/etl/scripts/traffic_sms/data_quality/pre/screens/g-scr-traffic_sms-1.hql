-------------------------------------------------------------------------------
--- NUMBER OF TRAFFIC_SMS NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of traffic_sms with
---              one of its values as a null value
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'traffic_sms' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=traffic_sms;
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
        IF ( country_id               IS NULL OR
             month_id                 IS NULL OR
             billing_cycle_dt         IS NULL OR
             billing_cycle_id         IS NULL OR
             subscription_id          IS NULL OR
             msisdn_id                IS NULL OR
             day_cd                   IS NULL OR
             time_range_cd            IS NULL OR
             imei_num                 IS NULL OR
             sms_offnet_out_qt        IS NULL OR
             sms_onnet_out_qt         IS NULL OR
             sms_international_out_qt IS NULL OR
             sms_roaming_out_qt       IS NULL OR
             sms_offnet_in_qt         IS NULL OR
             sms_onnet_in_qt          IS NULL OR
             sms_international_in_qt  IS NULL OR
             sms_roaming_in_qt        IS NULL OR
             sms_out_bundled_rv       IS NULL OR
             sms_out_not_bundled_rv   IS NULL OR
             sms_roaming_out_rv       IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_traffic_sms
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
