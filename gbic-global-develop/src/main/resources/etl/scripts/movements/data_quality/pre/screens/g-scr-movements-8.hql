-------------------------------------------------------------------------------
--- # DUPLICATED PRIMARY KEYS (CCOUNTRY_ID, MONTH_ID, CUSTOMER_ID, MSISDN_ID,
---                            SUBSCRIPTION_ID, ACTIVATION_DT, MOVEMENT_ID,
---                            MOVEMENT_DT, MOVEMENT_CHANNEL_ID, CAMPAIGN_ID,
---                            SEGMENT_CD, PRE_POST_ID, PREV_PRE_POST_ID,
---                            TARIFF_PLAN_ID, PREV_TARIFF_PLAN_ID,
---                            PROD_TYPE_CD, PORT_OP_CD)
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of pk duplicates in movements's file
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 8 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=8;

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
        prod_type_cd, port_op_cd,
        IF ( count(*) > 1, 'ko', 'ok' ) AS count_pk
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
        prod_type_cd, port_op_cd
) x
GROUP BY count_pk;
