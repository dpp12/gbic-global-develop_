-------------------------------------------------------------------------------------------
---                          Global Business Model                                      ---
-------------------------------------------------------------------------------------------
--- Description: Script to calculate mobile tariffs dimension                           ---
---                                                                                     ---
--- Parameters:                                                                         ---
---      targetOb:  Gbic global identifier of the country to calculate the output       ---
---      nominalTime:  Month of data to take into account for the query                 ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar targetOb=1                                                      ---
---           --hivevar nominalTime=2015-01-01                                          ---
---           -f dims_m_tariffs.sql                                                     ---
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

SET hive.execution.engine=tez;
SET hive.auto.convert.join=false;

FROM
    (SELECT
        h.*,
        IF(max_date_end IS NOT NULL AND h.date_end == h.max_date_end,1,0) AS last_state,
        d.tariff_plan_id,
        d.activation_date,
        d.deactivation_date
      FROM
         (SELECT 
            *,
            max(date_end) OVER (PARTITION BY gbic_op_id,gbic_tariff_id) as max_date_end
         FROM 
            {{ project.prefix }}gbic_global_bnss.dims_m_tariffs_history
         WHERE 
            gbic_op_id = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
         ) h
         INNER JOIN
         (SELECT 
            *
          FROM 
            {{ project.prefix }}gbic_global_bnss.dims_m_tariffs
          WHERE 
            gbic_op_id = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
         )d
         ON
            h.gbic_op_id = d.gbic_op_id AND
            h.gbic_tariff_id = d.gbic_tariff_id
    ) old
    FULL OUTER JOIN
    (SELECT
        tp.*,
         IF(i.tariff_index IS NOT NULL,i.tariff_index,0) AS tariff_index
     FROM
        (SELECT
            gbic_op_id,
            tariff_plan_id,
            des_plan,
            data_tariff_ind as is_data_tariff
         FROM
            {{ project.prefix }}gbic_global.gbic_global_dim_m_tariff_plan
         WHERE
            gbic_op_id = ${targetOb} AND
            month='${nominalTime}'
         ) tp
          LEFT OUTER JOIN
         (SELECT
            max(gbic_tariff_id) AS tariff_index,
            gbic_op_id
          FROM   
            {{ project.prefix }}gbic_global_bnss.dims_m_tariffs
          WHERE 
            gbic_op_id = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
          GROUP BY 
            gbic_op_id
          )i
          ON 
            tp.gbic_op_id=i.gbic_op_id
     )new
     ON 
        old.gbic_op_id = new.gbic_op_id AND
        old.tariff_plan_id = new.tariff_plan_id

----------------
-- SCD Type 1 --
----------------
-- Existing records
INSERT OVERWRITE TABLE dims_m_tariffs
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    new.gbic_op_id,
    old.gbic_tariff_id,
    new.tariff_plan_id,
    old.activation_date,
    '9999-12-31' AS deactivation_date,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'E' AS hist_flag
WHERE
    old.gbic_op_id IS NOT NULL AND
    new.gbic_op_id IS NOT NULL AND
    old.last_state = 1

-- New records
INSERT OVERWRITE TABLE dims_m_tariffs
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    new.gbic_op_id,
    rank() OVER (PARTITION BY new.gbic_op_id ORDER BY new.tariff_plan_id ASC) + new.tariff_index AS gbic_tariff_id,
    new.tariff_plan_id,
    '${nominalTime}' AS activation_date,
    '9999-12-31' AS deactivation_date,
    new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'N' AS hist_flag
WHERE
    old.gbic_op_id IS NULL 
 
-- Stored records
INSERT OVERWRITE TABLE dims_m_tariffs
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    old.gbic_op_id,
    old.gbic_tariff_id,
    old.tariff_plan_id,
    old.activation_date,
    date_sub('${nominalTime}',1) AS deactivation_date,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'S' AS hist_flag
WHERE
    old.gbic_op_id IS NOT NULL AND
    new.gbic_op_id IS NULL AND
    old.last_state = 1
    
    
----------------
-- SCD Type 2 --
----------------
-- Remained records. Existing record that does not changes
INSERT OVERWRITE TABLE dims_m_tariffs_history 
PARTITION ( gbic_op_id_pt, month_pt, hist_flag ) 
SELECT 
    old.gbic_op_id,
    old.gbic_tariff_id,
    old.date_ini,
    old.date_end,
    old.des_plan,
    old.is_data_tariff,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'R' AS hist_flag  
WHERE
    new.des_plan == old.des_plan AND
    new.is_data_tariff == old.is_data_tariff AND  
    old.last_state = 1
    
-- Stored record. Stores old value of a record which is updated or removed
INSERT OVERWRITE TABLE dims_m_tariffs_history  
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )  
SELECT 
    old.gbic_op_id,
    old.gbic_tariff_id,
    old.date_ini,
    date_sub('${nominalTime}',1) AS date_end,
    old.des_plan,
    old.is_data_tariff,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'S' AS hist_flag  
WHERE
    new.gbic_op_id IS NULL
    
-- Updated record. Stores new value of a record which is updated
INSERT OVERWRITE TABLE dims_m_tariffs_history 
PARTITION ( gbic_op_id_pt, month_pt, hist_flag ) 
SELECT
    new.gbic_op_id,
    old.gbic_tariff_id,
    '${nominalTime}' AS date_ini,
    '9999-12-31' AS date_end,
    new.des_plan,
    new.is_data_tariff,
    new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'U' AS hist_flag 
WHERE
    old.gbic_op_id IS NOT NULL AND
    old.last_state = 1 AND
    (new.des_plan != old.des_plan OR
    new.is_data_tariff != old.is_data_tariff)
    
-- New records. Record that did not exist
INSERT OVERWRITE TABLE dims_m_tariffs_history  
PARTITION ( gbic_op_id_pt, month_pt, hist_flag ) 
SELECT
    new.gbic_op_id,
    rank() OVER (PARTITION BY new.gbic_op_id ORDER BY new.tariff_plan_id ASC) + new.tariff_index AS gbic_tariff_id,
    '${nominalTime}' AS date_ini,
    '9999-12-31' AS date_end,
    new.des_plan,
    new.is_data_tariff,
    new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'N' AS hist_flag 
WHERE
    old.gbic_op_id IS NULL
;
