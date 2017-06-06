-------------------------------------------------------------------------------
--- NUMBER OF M_LINES NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of m_lines with one of its values
---              as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_lines;
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
        IF ( country_id         IS NULL OR
             month_id           IS NULL OR
             msisdn_id          IS NULL OR
             subscription_id    IS NULL OR
             imsi_id            IS NULL OR
             customer_id        IS NULL OR
             mobile_customer_id IS NULL OR
             party_type_cd      IS NULL OR
             activation_dt      IS NULL OR
             prod_type_cd       IS NULL OR
             imei_num           IS NULL OR
             line_status_cd     IS NULL OR
             segment_cd         IS NULL OR
             pre_post_id        IS NULL OR
             account_id         IS NULL OR
             tariff_plan_id     IS NULL OR
             billing_cycle_id   IS NULL OR
             postal_cd          IS NULL OR
             multisim_ind       IS NULL OR
             exceed_ind         IS NULL OR
             data_tariff_ind    IS NULL OR
             extra_data_num     IS NULL OR
             extra_data_rv      IS NULL OR
             extra_data_qt      IS NULL OR
             ppu_num            IS NULL OR
             ppu_rv             IS NULL OR
             ppu_qt             IS NULL OR
             data_consumed_qt   IS NULL OR
             data_bundled_qt    IS NULL OR
             call_voice_qt      IS NULL OR
             voice_consumed_qt  IS NULL OR
             sms_consumed_qt    IS NULL OR
             prepaid_top_up_id  IS NULL OR
             top_up_cost_num    IS NULL OR
             top_up_cost_rv     IS NULL OR
             top_up_promo_num   IS NULL OR
             top_up_promo_rv    IS NULL OR
             no_top_up_rv       IS NULL OR
             total_rv           IS NULL OR
             bta_ind            IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_m_lines
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
