-------------------------------------------------------------------------------
--- NUMBER OF F_LINES NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of f_lines with
---              one of its values as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_lines;
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
        IF ( country_id           IS NULL OR
             month_id             IS NULL OR
             subscription_id      IS NULL OR
             administrator_id     IS NULL OR
             customer_id          IS NULL OR
             fix_customer_id      IS NULL OR
             postal_cd            IS NULL OR
             party_type_cd        IS NULL OR
             segment_cd           IS NULL OR
             voice_ind            IS NULL OR
             voice_activation_dt  IS NULL OR
             voice_type_cd        IS NULL OR
             voice_tariff_plan_id IS NULL OR
             voice_month_rv       IS NULL OR
             bband_ind            IS NULL OR
             bband_activation_dt  IS NULL OR
             bband_type_cd        IS NULL OR
             bband_tariff_plan_id IS NULL OR
             speed_band_qt        IS NULL OR
             bband_month_rv       IS NULL OR
             tv_ind               IS NULL OR
             tv_sales_dt          IS NULL OR
             tv_activation_dt     IS NULL OR
             tv_use_dt            IS NULL OR
             tv_promo_id          IS NULL OR
             tv_end_promo_dt      IS NULL OR
             tv_type_cd           IS NULL OR
             tv_tariff_plan_id    IS NULL OR
             tv_points_qt         IS NULL OR
             tv_recurring_rv      IS NULL OR
             tv_non_recurring_rv  IS NULL OR
             tv_month_rv          IS NULL OR
             workstation_ind      IS NULL OR
             workstation_type_cd  IS NULL OR
             app_ind              IS NULL OR
             total_month_rv       IS NULL OR
             data_consumed_qt     IS NULL OR
             calls_voice_qt       IS NULL OR
             minutes_voice_qt     IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_f_lines
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
