-------------------------------------------------------------------------------------------
---                          Global Business Model                                      ---
-------------------------------------------------------------------------------------------
--- Description: Script to calculate customers dimension                                ---
---                                                                                     ---
--- Parameters:                                                                         ---
---      targetOb:  Gbic global identifier of the country to calculate the output       ---
---      nominalTime:  Month of data to take into account for the query                 ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar targetOb=1                                                      ---
---           --hivevar nominalTime=2015-01-01                                          ---
---           -f dims_customers.sql                                                     ---
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
        d.customer_id,
        d.party_identification_num,
        d.creation_date,
        d.activation_date,
        d.deactivation_date
     FROM
        (SELECT 
            *,
            max(date_end) OVER (PARTITION BY gbic_op_id,gbic_customer_id,ob_id) as max_date_end
         FROM 
            {{ project.prefix }}gbic_global_bnss.dims_customers_history
         WHERE 
            gbic_op_id = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01') 
         ) h
         INNER JOIN
         (SELECT 
            *
          FROM 
            {{ project.prefix }}gbic_global_bnss.dims_customers
          WHERE 
            gbic_op_id = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01') 
         )d
         ON
            h.gbic_op_id = d.gbic_op_id AND
            h.ob_id = d.ob_id AND
            h.gbic_customer_id = d.gbic_customer_id
    ) old
    FULL OUTER JOIN
    (SELECT
        c.*,
        IF(i.customer_index IS NOT NULL,i.customer_index,0) AS customer_index
     FROM
        (SELECT
            * 
         FROM
            {{ project.prefix }}gbic_global.gbic_global_customer
         WHERE
            gbic_op_id = ${targetOb} AND
            month='${nominalTime}' AND
            seg_global_id IN (1,2,3,4,5,7,8,9)
         )c
         LEFT OUTER JOIN
         (SELECT
            max(gbic_customer_id) AS customer_index,
            gbic_op_id_pt AS gbic_op_id,
            ob_id
          FROM   
            {{ project.prefix }}gbic_global_bnss.dims_customers
          WHERE 
            gbic_op_id_pt = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
          GROUP BY
            gbic_op_id_pt,
            ob_id
          )i 
          ON 
            c.gbic_op_id = i.gbic_op_id AND
            c.ob_id = i.ob_id
     )new
     ON 
        old.gbic_op_id = new.gbic_op_id AND
        old.customer_id = new.customer_id AND
        old.ob_id = new.ob_id

----------------
-- SCD Type 1 --
----------------
-- Existing records
INSERT OVERWRITE TABLE dims_customers
PARTITION  ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    new.gbic_op_id,
    old.gbic_customer_id,
    new.customer_id,
    new.ob_id,
    new.party_identification_num,
    new.birth_dt,
    new.activation_dt,
    '9999-12-31' AS deactivation_date,
    old.gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'E' AS hist_flag
WHERE
    old.gbic_op_id IS NOT NULL AND
    new.gbic_op_id IS NOT NULL AND
    old.last_state = 1
    
-- New records
INSERT OVERWRITE TABLE dims_customers
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    new.gbic_op_id,
    rank() OVER (PARTITION BY new.gbic_op_id ORDER BY new.customer_id ASC,new.ob_id ASC) + new.customer_index AS gbic_customer_id,
    new.customer_id,
    new.ob_id,
    new.party_identification_num,
    new.birth_dt,
    new.activation_dt,
    '9999-12-31' AS deactivation_date,
    new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'N' AS hist_flag
WHERE
    old.gbic_op_id IS NULL
    
-- Stored records
INSERT OVERWRITE TABLE dims_customers
PARTITION  ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    old.gbic_op_id,
    old.gbic_customer_id,
    old.customer_id,
    old.ob_id,
    old.party_identification_num,
    old.creation_date,
    old.activation_date,
    date_sub('${nominalTime}',1) AS deactivation_date,
    old.gbic_op_id_pt,
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
INSERT OVERWRITE TABLE dims_customers_history 
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT
    old.gbic_op_id,
    old.ob_id,
    old.gbic_customer_id,
    old.date_ini,
    old.date_end,
    old.seg_global_id,
    old.org_name,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'R' as hist_flag
WHERE
    new.seg_global_id = old.seg_global_id AND
    new.org_name = old.org_name AND
    old.last_state = 1
    
-- Stored record. Stores old value of a record which is updated or removed 
INSERT OVERWRITE TABLE dims_customers_history
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT
    old.gbic_op_id,
    old.ob_id,
    old.gbic_customer_id,
    old.date_ini,
    IF( old.date_end == '9999-12-31',
            date_sub('${nominalTime}',1),
            old.date_end) AS date_end,
    old.seg_global_id,
    old.org_name,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'S' as hist_flag
WHERE
    new.gbic_op_id IS NULL
    
-- Updated record. Stores new value of a record which is updated 
INSERT OVERWRITE TABLE dims_customers_history
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT
    new.gbic_op_id,
    new.ob_id,
    old.gbic_customer_id,
    '${nominalTime}' AS date_ini,
    '9999-12-31' AS date_end,
    new.seg_global_id,
    new.org_name,
    new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'U' as hist_flag
WHERE
    new.gbic_op_id IS NOT NULL AND
    old.last_state = 1 AND
    (new.seg_global_id != old.seg_global_id OR
    new.org_name != old.org_name)
    
-- New records. Record that did not exist
INSERT OVERWRITE TABLE dims_customers_history
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT
    new.gbic_op_id,
    new.ob_id,
    rank() OVER (PARTITION BY new.gbic_op_id ORDER BY new.customer_id ASC,new.ob_id ASC) + new.customer_index AS gbic_customer_id,
    '${nominalTime}' AS date_ini,
    '9999-12-31' AS date_end,
    new.seg_global_id,
    new.org_name,
     new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'N' AS hist_flag
WHERE
    old.gbic_op_id IS NULL
;
