-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=bra;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=F_TARIFF_PLAN;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=201;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_f_tariff_plan (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    subscription_id               string COMMENT 'Unique identifier of the line',
    tariff_plan_id                string COMMENT 'Tariff Plan ID',
    tariff_plan_desc              string COMMENT 'Plan description '
) COMMENT 'Description and classification of the Tariff Plan'
PARTITIONED BY (
    gbic_op_id                    int    COMMENT 'GBIC GLOBAL OPERATOR ID',
    month                         string COMMENT 'Date of monthly files')
STORED AS ORC;

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
