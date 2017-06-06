-------------------------------------------------------------------------------------------
---                          Global Business Model                                      ---
-------------------------------------------------------------------------------------------
--- Description: Script to calculate devices dimension                                  ---
---                                                                                     ---
--- Parameters:                                                                         ---
---      nominalTime:  Month of data to take into account for the query                 ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar nominalTime=2015-01-01                                          ---
---           -f dims_tacs.sql                                                          ---
---                                                                                     ---
-------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
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
        d.des_manufact,
        d.des_model,
        d.os,
        d.version_os,
        d.technology,
        d.activation_date,
        d.deactivation_date
      FROM
         (SELECT 
            *
         FROM 
            {{ project.prefix }}gbic_global_bnss.dims_tacs_history
         WHERE 
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
         ) h
         INNER JOIN
         (SELECT 
            *
          FROM 
            {{ project.prefix }}gbic_global_bnss.dims_tacs
          WHERE 
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
         )d
         ON
            h.device_id = d.device_id
    ) old
    FULL OUTER JOIN
    (SELECT
        tac as device_id,
        des_manufact,
        concat_ws(',',des_model) AS des_model,
        market_category,
        tef_category,
        os,
        version_os,
        if(technology_4g_dl != 'No LTE', '4G',
            if(technology_3g != 'No', '3G',
                '2G')) AS technology
     FROM
        {{ project.prefix }}gbic_global.gbic_global_tacs
     WHERE
        month = '${nominalTime}'
     ) new
     ON 
        old.device_id = new.device_id

----------------
-- SCD Type 1 --
----------------
INSERT OVERWRITE TABLE dims_tacs
PARTITION ( month_pt )
SELECT 
    IF(new.device_id IS NOT NULL, new.device_id, old.device_id) AS device_id,
    IF(new.device_id IS NOT NULL, new.des_manufact, old.des_manufact) AS des_manufact,
    IF(new.device_id IS NOT NULL, new.des_model, old.des_model) AS des_model,
    IF(new.device_id IS NOT NULL, new.os, old.os) AS os,
    IF(new.device_id IS NOT NULL, new.version_os, old.version_os) AS version_os,
    IF(new.device_id IS NOT NULL, new.technology, old.technology) AS technology,
    IF(new.device_id IS NOT NULL, '${nominalTime}', old.activation_date) AS activation_date,
    IF(new.device_id IS NOT NULL, 
            '9999-12-31', 
            IF (old.deactivation_date=='9999-12-31',
                    date_sub('${nominalTime}',1),
                    old.deactivation_date)) AS deactivation_date,
    '${nominalTime}' AS month_pt        

----------------
-- SCD Type 2 --
----------------
-- Unchaged records
INSERT OVERWRITE TABLE dims_tacs_history
PARTITION ( month_pt, hist_flag ) 
SELECT
    old.device_id,
    old.date_ini,
    old.date_end,
    old.market_category,
    old.tef_category,
    '${nominalTime}' AS month_pt,
    'U' as hist_flag   
WHERE
    new.market_category == old.market_category AND
    new.tef_category == old.tef_category  
    
-- Closed records. 
INSERT OVERWRITE TABLE dims_tacs_history 
PARTITION ( month_pt, hist_flag )  
SELECT
    old.device_id,
    old.date_ini,
    date_sub('${nominalTime}',1) AS date_end,
    old.market_category,
    old.tef_category,
    '${nominalTime}' AS month_pt,
    'C' as hist_flag
WHERE
    new.device_id IS NULL OR 
    new.market_category != old.market_category OR
    new.tef_category != old.tef_category
    
-- Changed or added records
INSERT OVERWRITE TABLE dims_tacs_history  
PARTITION ( month_pt, hist_flag ) 
SELECT
    new.device_id,
    '${nominalTime}' AS date_ini,
    '9999-12-31' AS date_end,
    new.market_category,
    new.tef_category,
    '${nominalTime}' AS month_pt,
    'N' as hist_flag    
WHERE
    old.device_id IS NULL OR 
    new.market_category != old.market_category OR
    new.tef_category != old.tef_category
;
