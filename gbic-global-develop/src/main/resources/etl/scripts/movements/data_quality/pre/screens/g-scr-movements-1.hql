-------------------------------------------------------------------------------
--- NUMBER OF MOVEMENTS NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with
---              one of its values as a null value
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
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
        IF ( country_id          IS NULL OR
             month_id            IS NULL OR
             customer_id         IS NULL OR
             msisdn_id           IS NULL OR
             subscription_id     IS NULL OR
             activation_dt       IS NULL OR
             movement_id         IS NULL OR
             movement_dt         IS NULL OR
             movement_channel_id IS NULL OR
             campaign_id         IS NULL OR
             segment_cd          IS NULL OR
             pre_post_id         IS NULL OR
             prev_pre_post_id    IS NULL OR
             tariff_plan_id      IS NULL OR
             prev_tariff_plan_id IS NULL OR
             prod_type_cd        IS NULL OR
             port_op_cd          IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_movements
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
