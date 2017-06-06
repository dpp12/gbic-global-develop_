-------------------------------------------------------------------------------
--- NUMBER OF IMEI_SALES NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of imei_sales with one of its values
---              as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'imei_sales' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=imei_sales;
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
        IF ( country_id              IS NULL OR
             month_id                IS NULL OR
             msisdn_id               IS NULL OR
             imei_num                IS NULL OR
             pre_post_id             IS NULL OR
             segment_cd              IS NULL OR
             activation_movement     IS NULL OR
             tariff_plan_id          IS NULL OR
             channel_cd              IS NULL OR
             sales_network_cd        IS NULL OR
             distribution_channel_cd IS NULL OR
             campain_cd              IS NULL OR
             sale_price              IS NULL OR
             purchase_price          IS NULL OR
             financial_support       IS NULL OR
             postal_cd               IS NULL OR
             device_name             IS NULL OR
             imei_origin             IS NULL OR
             subscription_id         IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_imei_sales
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
