-------------------------------------------------------------------------------
--- NUMBER OF TRAFFIC_VOICE NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of traffic_voice with
---              one of its values as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file.'traffic_voice' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=traffic_voice;
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
        IF ( country_id                 IS NULL OR
             month_id                   IS NULL OR
             billing_cycle_dt           IS NULL OR
             billing_cycle_id           IS NULL OR
             subscription_id            IS NULL OR
             msisdn_id                  IS NULL OR
             day_cd                     IS NULL OR
             time_range_cd              IS NULL OR
             imei_num                   IS NULL OR
             call_offnet_fixed_out      IS NULL OR
             call_onnet_fixed_out       IS NULL OR
             call_offnet_mobile_out     IS NULL OR
             call_onnet_mobile_out      IS NULL OR
             call_international_out     IS NULL OR
             call_onnet_out_free        IS NULL OR
             call_onnet_rcm_out         IS NULL OR
             call_roaming_out           IS NULL OR
             call_out_special_numbers   IS NULL OR
             call_fixed_in              IS NULL OR
             call_offnet_mobile_in      IS NULL OR
             call_onnet_mobile_in       IS NULL OR
             call_roaming_in            IS NULL OR
             call_international_in      IS NULL OR
             min_offnet_fixed_out       IS NULL OR
             min_onnet_fixed_out        IS NULL OR
             min_offnet_mobile_out      IS NULL OR
             min_onnet_mobile_out       IS NULL OR
             min_international_out      IS NULL OR
             min_onnet_free_out         IS NULL OR
             min_onnet_rcm_out          IS NULL OR
             min_roaming_out            IS NULL OR
             min_out_special_numbers    IS NULL OR
             min_fixed_in               IS NULL OR
             min_offnet_mobile_in       IS NULL OR
             min_onnet_mobile_in        IS NULL OR
             min_roaming_in             IS NULL OR
             min_international_in       IS NULL OR
             min_fixed_out_bundled      IS NULL OR
             min_mobile_out_bundled     IS NULL OR
             min_fixed_out_not_bundled  IS NULL OR
             min_mobile_out_not_bundled IS NULL OR
             min_fixed_out_exceed       IS NULL OR
             min_mobile_out_exceed      IS NULL OR
             roaming_rv                 IS NULL OR
             out_other_rv               IS NULL OR
             out_national_onnet_rv      IS NULL OR
             out_national_offnet_rv     IS NULL OR
             out_national_fixed_rv      IS NULL OR
             out_international_rv       IS NULL OR
             call_2g_out                IS NULL OR
             call_3g_out                IS NULL OR
             call_4g_out                IS NULL OR
             call_2g_in                 IS NULL OR
             call_3g_in                 IS NULL OR
             call_4g_in                 IS NULL OR
             min_2g_out                 IS NULL OR
             min_3g_out                 IS NULL OR
             min_4g_out                 IS NULL OR
             min_2g_in                  IS NULL OR
             min_3g_in                  IS NULL OR
             min_4g_in                  IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_traffic_voice
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
