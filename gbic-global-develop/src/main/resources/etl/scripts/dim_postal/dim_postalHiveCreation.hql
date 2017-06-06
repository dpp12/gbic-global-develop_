-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=DIM_POSTAL;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_dim_postal (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    postal_id                     string COMMENT 'Postal code identifier',
    location_level_local_cd       string COMMENT 'Location level (local code). p.e. PRV',
    location_level_local_name     string COMMENT 'Location level (local name). p.e. PROVINCIA',
    location_level_global_id      int    COMMENT 'Location level (global id)',
    location_level_global_name    string COMMENT 'Location level (global name)',
    location_name                 string COMMENT 'Location name. p.e. PONTEVEDRA'
) COMMENT 'Description and classification of the postal code (locations)'
PARTITIONED BY (
    gbic_op_id                    int    COMMENT 'GBIC GLOBAL OPERATOR ID',
    month                         string COMMENT 'Date of monthly files')
STORED AS ORC;

-- ********************************
-- VIEWS
-- ********************************
DROP VIEW IF EXISTS gbic_global_dim_postal_view;

CREATE VIEW gbic_global_dim_postal_view AS
SELECT
    gbic_op_id,
    month,
    postal_id,
    IF(collect_set(loc_lev_7)[0] IS NOT NULL,collect_set(loc_lev_7)[0],'') AS loc_lev_7,
    IF(collect_set(loc_lev_6)[0] IS NOT NULL,collect_set(loc_lev_6)[0],'') AS loc_lev_6,
    IF(collect_set(loc_lev_5)[0] IS NOT NULL,collect_set(loc_lev_5)[0],'') AS loc_lev_5,
    IF(collect_set(loc_lev_4)[0] IS NOT NULL,collect_set(loc_lev_4)[0],'') AS loc_lev_4
FROM (
    SELECT
        gbic_op_id,
        month,
        postal_id,
        if(location_level_global_id=7,location_name,NULL) AS loc_lev_7,
        if(location_level_global_id=6,location_name,NULL) AS loc_lev_6,
        if(location_level_global_id=5,location_name,NULL) AS loc_lev_5,
        if(location_level_global_id=4,location_name,NULL) AS loc_lev_4
    FROM
        gbic_global_dim_postal
) PIVOT
GROUP BY
    gbic_op_id,
    month,
    postal_id;

-------------------------------------------------------------------------------
-- STAGING AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global_staging;

USE {{ project.prefix }}gbic_global_staging;

CREATE TABLE IF NOT EXISTS gbic_global_{{ item }}
LIKE {{ project.prefix }}gbic_global.gbic_global_{{ item }};

ALTER TABLE gbic_global_{{ item }}
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);
