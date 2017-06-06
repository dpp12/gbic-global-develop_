-------------------------------------------------------------------------------------------
---                          Global Business Model                                      ---
-------------------------------------------------------------------------------------------
--- Description: Script to calculate basic fix kpis.                                    ---
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
---           -f kpis_fix.sql                                                           ---
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
          WHEN bband_activation_dt is null then -1
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=30 THEN 0
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=90 THEN 1
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=180 THEN 3
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=365 THEN 6
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=365*2 THEN 12
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=365*3 THEN 24
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=365*4 THEN 36
          WHEN datediff(concat(substr(date_add(month,31),0,7),'-01'),bband_activation_dt)<=365*5 THEN 48
          else 60
        END AS months_old 
     FROM 
        {{ project.prefix }}gbic_global.gbic_global_f_lines 
     WHERE 
        gbic_op_id = ${targetOb} AND
        month = '${nominalTime}'
    ) lin
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
        month_pt = '${nominalTime}' AND ob_id!='M'
    )  cus
    ON 
        lin.gbic_op_id = cus.gbic_op_id AND
        lin.month = cus.month AND
        lin.customer_id = cus.customer_id
    LEFT OUTER JOIN
    (SELECT
        gbic_op_id,
        month_pt       AS month,
        gbic_tariff_id AS gbic_bband_tariff_id,
        tariff_plan_id AS bband_tariff_plan_id
      FROM   
        {{ project.prefix }}gbic_global_bnss.dims_f_tariffs
      WHERE 
        gbic_op_id_pt = ${targetOb} AND
        month_pt = '${nominalTime}'
    ) bband_tariff
    ON 
        lin.gbic_op_id = bband_tariff.gbic_op_id AND
        lin.month = bband_tariff.month AND
        lin.bband_tariff_plan_id = bband_tariff.bband_tariff_plan_id
    LEFT OUTER JOIN
    (SELECT
        gbic_op_id,
        month_pt       AS month,
        gbic_tariff_id AS gbic_voice_tariff_id,
        tariff_plan_id AS voice_tariff_plan_id
      FROM   
        {{ project.prefix }}gbic_global_bnss.dims_f_tariffs
      WHERE 
        gbic_op_id_pt = ${targetOb} AND
        month_pt = '${nominalTime}'
    ) voice_tariff
    ON 
        lin.gbic_op_id = voice_tariff.gbic_op_id AND
        lin.month = voice_tariff.month AND
        lin.voice_tariff_plan_id = voice_tariff.voice_tariff_plan_id
    LEFT OUTER JOIN
    (SELECT
        gbic_op_id,
        month_pt       AS month,
        gbic_tariff_id AS gbic_tv_tariff_id,
        tariff_plan_id AS tv_tariff_plan_id
      FROM   
        {{ project.prefix }}gbic_global_bnss.dims_f_tariffs
      WHERE 
        gbic_op_id_pt = ${targetOb} AND
        month_pt = '${nominalTime}'
    ) tv_tariff
    ON 
        lin.gbic_op_id = tv_tariff.gbic_op_id AND
        lin.month = tv_tariff.month AND
        lin.tv_tariff_plan_id = tv_tariff.tv_tariff_plan_id
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
    
INSERT OVERWRITE TABLE kpis_fix
PARTITION (gbic_op_id_pt, month_pt, seg_global_id_pt)
SELECT 
    lin.gbic_op_id, 
    seg_global_id,
    IF (seg_global_id IN (1,2,3,4,5,7,8,9) AND gbic_customer_id IS NOT NULL, gbic_customer_id, -1) AS gbic_customer_id,
    bband_ind,
    IF (gbic_bband_tariff_id IS NOT NULL, gbic_bband_tariff_id, -1),
    IF (gbic_voice_tariff_id IS NOT NULL, gbic_voice_tariff_id, -1),
    IF (gbic_tv_tariff_id IS NOT NULL, gbic_tv_tariff_id, -1),
    IF (gbic_geo_zone_id IS NOT NULL, gbic_geo_zone_id, -1),
    months_old,
    lin.bband_type_cd,
    lin.speed_band_qt,
    lin.month,
    count(distinct lin.subscription_id) AS n_lines_total,
    sum(bband_month_rv) AS bband_month_rv,
    sum(total_month_rv) AS total_month_rv,
    gbic_op_id AS gbic_op_id_pt,
    month AS month_pt,
    seg_global_id AS seg_global_id_pt
GROUP BY
    lin.gbic_op_id, 
    seg_global_id,
    IF (seg_global_id IN (1,2,3,4,5,7,8,9) AND gbic_customer_id IS NOT NULL, gbic_customer_id, -1),
    bband_ind,
    IF (gbic_bband_tariff_id IS NOT NULL, gbic_bband_tariff_id, -1),
    IF (gbic_voice_tariff_id IS NOT NULL, gbic_voice_tariff_id, -1),
    IF (gbic_tv_tariff_id IS NOT NULL, gbic_tv_tariff_id, -1),
    IF (gbic_geo_zone_id IS NOT NULL, gbic_geo_zone_id, -1),
    months_old,
    lin.bband_type_cd,
    lin.speed_band_qt,
    lin.month;

