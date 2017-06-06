-------------------------------------------------------------------------------------------
---                          Global Business Model                                      ---
-------------------------------------------------------------------------------------------
--- Description: Script to calculate basic mobile kpis.                                 ---
---              This script depends on the execution of dimensions so must be run for  ---
---              a month and a country always after generating its tariffs, tacs and    ---
---              customers dimensions.                                                  ---
---                                                                                     ---
--- Parameters:                                                                         ---
---      targetOb:  Gbic global identifier of the country to calculate the output       ---
---      nominalTime:  Month of data to take into account for the query                 ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar targetOb=1                                                      ---
---           --hivevar nominalTime=2015-01-01                                          ---
---           -f kpis_mobile.sql                                                        ---
---                                                                                     ---
-------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- set hivevar:targetOb = 1;
-- set hivevar:nominalTime = 2015-01-01;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
USE {{ project.prefix }}gbic_global_bnss;

SET hive.execution.engine=mr;
SET hive.auto.convert.join=false;

FROM
    (SELECT 
        *,
        CASE
          WHEN activation_dt is null then -1
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=30 THEN 0
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=90 THEN 1
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=180 THEN 3
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=365 THEN 6
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=365*2 THEN 12
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=365*3 THEN 24
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=365*4 THEN 36
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),activation_dt)<=365*5 THEN 48
          else 60
        END AS months_old 
     FROM 
        {{ project.prefix }}gbic_global.gbic_global_m_lines 
     WHERE 
        gbic_op_id = ${targetOb} AND
        month = '${nominalTime}'
    ) lin
    LEFT OUTER JOIN
    (SELECT 
        *
     FROM
        {{ project.prefix }}gbic_global.gbic_global_invoice
     WHERE
        gbic_op_id = ${targetOb} AND
        month = '${nominalTime}'
    ) inv
    ON
        lin.gbic_op_id = inv.gbic_op_id AND
        lin.month = inv.month AND
        lin.msisdn_id = inv.msisdn_id AND
        lin.subscription_id = inv.subscription_id
    LEFT OUTER JOIN
    (SELECT 
        gbic_op_id,
        month_pt AS month,
        gbic_customer_id,
        customer_id
      FROM 
        {{ project.prefix }}gbic_global_bnss.dims_customers
      WHERE 
        gbic_op_id_pt = ${targetOb} AND
        month_pt = '${nominalTime}' AND ob_id!='F'
    )  cus
    ON 
        lin.gbic_op_id = cus.gbic_op_id AND
        lin.month = cus.month AND
        lin.customer_id = cus.customer_id
    LEFT OUTER JOIN
    (SELECT
        gbic_op_id,
        month_pt AS month,
        gbic_tariff_id,
        tariff_plan_id
      FROM   
        {{ project.prefix }}gbic_global_bnss.dims_m_tariffs
      WHERE 
        gbic_op_id_pt = ${targetOb} AND
        month_pt = '${nominalTime}'
    ) tfs
    ON 
        lin.gbic_op_id = tfs.gbic_op_id AND
        lin.month = tfs.month AND
        lin.tariff_plan_id = tfs.tariff_plan_id
    LEFT OUTER JOIN
    (SELECT
        dp.gbic_op_id,
        dp.postal_id AS postal_cd,
        dp.month,
        gz.gbic_geo_zone_id
     FROM
        (SELECT
            *
          FROM   
            {{ project.prefix }}gbic_global_bnss.dims_geo_zones
          WHERE 
            gbic_op_id_pt = ${targetOb} AND
            month_pt = '${nominalTime}'
        )gz
        INNER JOIN
        (SELECT
            *
         FROM   
            {{ project.prefix }}gbic_global.gbic_global_dim_postal_view
         WHERE 
            gbic_op_id = ${targetOb} AND
            month = '${nominalTime}'
        ) dp
        ON
          dp.gbic_op_id = gz.gbic_op_id_pt AND
          dp.month = gz.month_pt AND
          dp.loc_lev_7 = gz.loc_lev_7 AND
          dp.loc_lev_6 = gz.loc_lev_6 AND
          dp.loc_lev_5 = gz.loc_lev_5 AND
          dp.loc_lev_4 = gz.loc_lev_4
    ) geo
    ON 
        lin.gbic_op_id = geo.gbic_op_id AND
        lin.month = geo.month AND
        lin.postal_cd = geo.postal_cd
    
INSERT OVERWRITE TABLE kpis_mobile
PARTITION (gbic_op_id_pt, month_pt, seg_global_id_pt)
SELECT 
    lin.gbic_op_id, 
    seg_global_id,
    pre_post_id,
    IF (seg_global_id IN (1,2,3,4,5,7,8,9) AND gbic_customer_id IS NOT NULL, gbic_customer_id, -1) as gbic_customer_id,
    tac_id AS device_id,
    IF (gbic_tariff_id IS NOT NULL, gbic_tariff_id, -1) AS gbic_tariff_id,
    IF (gbic_geo_zone_id IS NOT NULL, gbic_geo_zone_id, -1),
    months_old,
    prod_type_cd,
    bta_ind,
    multisim_ind,
    lin.month,
    count(distinct lin.msisdn_id, lin.subscription_id) AS n_lines_total,
    sum(exceed_ind) AS n_lines_exceed,
    sum(data_tariff_ind) AS n_lines_data_tariff,
    sum(if(extra_data_num>0,1,0)) AS n_lines_extra_data,
    sum(if(extra_data_num>0 and exceed_ind=1,1,0)) AS n_lines_exceed_extra,
    sum(data_bundled_qt) AS vl_data_bundled,
    sum(if(exceed_ind==1,data_consumed_qt-data_bundled_qt,0)) AS vl_data_exceed,
    sum(data_consumed_qt) AS vl_data_consumed,
    sum(voice_consumed_qt) AS vl_voice_consumed,
    sum(sms_consumed_qt) AS vl_sms_consumed,
    sum(call_voice_qt) AS n_voice_calls,
    sum(IF(inv.quota_agg_rv IS NOT NULL,inv.quota_agg_rv,0)) AS quota_agg_rv,
    sum(IF(inv.quota_data_rv IS NOT NULL,inv.quota_data_rv,0)) AS quota_data_rv,
    sum(IF(inv.quota_voice_rv IS NOT NULL,inv.quota_voice_rv,0)) AS quota_voice_rv,
    sum(IF(inv.quota_mess_rv IS NOT NULL,inv.quota_mess_rv,0)) AS quota_mess_rv,
    sum(IF(inv.traffic_agg_rv IS NOT NULL,inv.traffic_agg_rv,0)) AS traffic_data_rv,
    sum(IF(inv.traffic_data_rv IS NOT NULL,inv.traffic_data_rv,0)) AS traffic_data_rv,
    sum(IF(inv.traffic_voice_rv IS NOT NULL,inv.traffic_voice_rv,0)) AS traffic_voice_rv,
    sum(IF(inv.traffic_mess_rv IS NOT NULL,inv.traffic_mess_rv,0)) AS traffic_mess_rv,
    sum(IF(inv.roaming_rv IS NOT NULL,inv.roaming_rv,0)) AS roaming_rv,
    sum(IF(inv.sva_rv IS NOT NULL,inv.sva_rv,0)) AS sva_rv,
    sum(IF(inv.packs_rv IS NOT NULL,inv.packs_rv,0)) AS packs_rv,
    sum(IF(inv.top_up_ex_rv IS NOT NULL,inv.top_up_ex_rv,0)) AS top_up_ex_rv,
    sum(IF(inv.top_up_co_rv IS NOT NULL,inv.top_up_co_rv,0)) AS top_up_co_rv,
    sum(IF(inv.gb_camp_rv IS NOT NULL,inv.gb_camp_rv,0)) AS gb_camp_rv,
    sum(IF(inv.others_rv IS NOT NULL,inv.others_rv,0)) AS others_rv,
    sum(IF(inv.tot_rv IS NOT NULL,inv.tot_rv,0)) AS tot_rv, 
    sum(IF(inv.top_up_rv IS NOT NULL,inv.top_up_rv,0)) AS top_up_rv,
    sum(IF(inv.itx_rv IS NOT NULL,inv.itx_rv,0)) AS itx_rv,
    sum(IF(inv.exp_itx_rv IS NOT NULL,inv.exp_itx_rv,0)) AS exp_itx_rv,
    sum(IF(extra_data_rv IS NOT NULL,extra_data_rv,0)) AS extra_data_rv,
    sum(IF(inv.total_invoice_rv IS NOT NULL,inv.total_invoice_rv,0)) AS total_invoice_rv,
    gbic_op_id AS gbic_op_id_pt,
    month AS month_pt,
    seg_global_id AS seg_global_id_pt
GROUP BY
    lin.gbic_op_id, 
    seg_global_id,
    pre_post_id,
    IF (seg_global_id IN (1,2,3,4,5,7,8,9) AND gbic_customer_id IS NOT NULL, gbic_customer_id, -1),
    tac_id,
    IF (gbic_tariff_id IS NOT NULL, gbic_tariff_id, -1),
    IF (gbic_geo_zone_id IS NOT NULL, gbic_geo_zone_id, -1),
    months_old,
    prod_type_cd,
    bta_ind,
    multisim_ind,
    lin.month;
