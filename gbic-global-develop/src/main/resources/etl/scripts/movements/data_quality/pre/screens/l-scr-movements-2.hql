-------------------------------------------------------------------------------
--- # DUPLICATED PRIMARY KEYS
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with duplicated
---              primary key
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'L' in this case
---     screenCounter: Part of the screen Id. 2 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=L;
-- SET hivevar:screenCounter=2;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'duplicated_fields'                        AS field,
    duplicated_fields                          AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
   SELECT
        country_id,
        month_id,
        customer_id,
        msisdn_id,
        subscription_id,
        activation_dt,
        movement_id,
        movement_dt,
        movement_channel_id,
        campaign_id,
        segment_cd,
        pre_post_id,
        prev_pre_post_id,
        tariff_plan_id,
        prev_tariff_plan_id,
        prod_type_cd,
        port_op_cd,
        count(*) AS n
    FROM gbic_dq_movements
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
    GROUP BY 
        country_id,
        month_id,
        customer_id,
        msisdn_id,
        subscription_id,
        activation_dt,
        movement_id,
        movement_dt,
        movement_channel_id,
        campaign_id,
        segment_cd,
        pre_post_id,
        prev_pre_post_id,
        tariff_plan_id,
        prev_tariff_plan_id,
        prod_type_cd,
        port_op_cd
    HAVING 
        n>1
    ) a;
