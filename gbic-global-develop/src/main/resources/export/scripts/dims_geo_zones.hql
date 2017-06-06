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
---           --hivevar targetOb=1                                                      ---
---           --hivevar nominalTime=2015-01-01                                          ---
---           -f dims_tacs.sql                                                          ---
---                                                                                     ---
-------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- set hivevar:targetOb = 1;
-- set hivevar:nominalTime = ${nominalTime};

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
USE {{ project.prefix }}gbic_global_bnss;

SET hive.execution.engine=tez;
SET hive.auto.convert.join=false;

FROM
    (SELECT 
        *
      FROM 
        {{ project.prefix }}gbic_global_bnss.dims_geo_zones
      WHERE 
        gbic_op_id_pt = ${targetOb} AND
        month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
    )  old
    FULL OUTER JOIN
    (SELECT DISTINCT
        c.gbic_op_id,
        c.month,
        loc_lev_7,
        loc_lev_6,
        loc_lev_5,
        loc_lev_4,
        IF(i.geo_zone_index IS NOT NULL,i.geo_zone_index,0) AS geo_zone_index
    FROM
        (SELECT DISTINCT
            gbic_op_id,
            month,
            loc_lev_7,
            loc_lev_6,
            loc_lev_5,
            loc_lev_4
         FROM 
            {{ project.prefix }}gbic_global.gbic_global_dim_postal_view
         WHERE
            gbic_op_id = ${targetOb} AND
            month = '${nominalTime}'
        ) c
        LEFT OUTER JOIN
         (SELECT
            max(gbic_geo_zone_id) AS geo_zone_index,
            gbic_op_id_pt AS gbic_op_id
          FROM   
            {{ project.prefix }}gbic_global_bnss.dims_geo_zones
          WHERE 
            gbic_op_id_pt = ${targetOb} AND
            month_pt = concat(substr(date_sub('${nominalTime}',1),0,7),'-01')
          GROUP BY
            gbic_op_id_pt
          )i 
          ON 
            c.gbic_op_id=i.gbic_op_id
     ) new
     ON     
        old.gbic_op_id = new.gbic_op_id AND
        old.loc_lev_7 = new.loc_lev_7 AND
        old.loc_lev_6 = new.loc_lev_6 AND
        old.loc_lev_5 = new.loc_lev_5 AND
        old.loc_lev_4 = new.loc_lev_4

----------------
-- SCD Type 1 --
----------------
-- Existing records
INSERT OVERWRITE TABLE dims_geo_zones
PARTITION  ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    old.gbic_op_id,
    old.gbic_geo_zone_id,
    old.loc_lev_7,
    old.loc_lev_6,
    old.loc_lev_5,
    old.loc_lev_4,
    old.activation_date,
    IF(new.gbic_op_id IS NOT NULL, 
            '9999-12-31', 
            IF (old.deactivation_date=='9999-12-31',
                    date_sub('${nominalTime}',1),
                    old.deactivation_date)) AS deactivation_date,
    old.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'E' AS hist_flag
WHERE
    old.gbic_op_id IS NOT NULL 

-- New records
INSERT OVERWRITE TABLE dims_geo_zones
PARTITION ( gbic_op_id_pt, month_pt, hist_flag )
SELECT 
    new.gbic_op_id,
    dense_rank() OVER (PARTITION BY new.gbic_op_id ORDER BY new.loc_lev_7, new.loc_lev_6, new.loc_lev_5, new.loc_lev_4) + new.geo_zone_index AS gbic_geo_zone_id,
    new.loc_lev_7,
    new.loc_lev_6,
    new.loc_lev_5,
    new.loc_lev_4,
   '${nominalTime}' AS activation_date,
    '9999-12-31' AS deactivation_date,
    new.gbic_op_id AS gbic_op_id_pt,
    '${nominalTime}' AS month_pt,
    'N' AS hist_flag
WHERE
    old.gbic_op_id IS NULL
;
