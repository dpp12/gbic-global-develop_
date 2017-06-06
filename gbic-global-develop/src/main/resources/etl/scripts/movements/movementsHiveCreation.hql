-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=MOVEMENTS;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_movements (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    customer_id                   string COMMENT 'Customer ifentifier',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    subscription_id               string COMMENT 'Subscription identifier, encrypted as a String',
    activation_dt                 string COMMENT 'Discharge date line',
    movement_dt                   string COMMENT 'Day movement',
    movement_id                   string COMMENT 'Movement identifier',
    movement_des                  string COMMENT 'Movement description',
    count_movement_qt             int    COMMENT 'Quantity to be computed to movement type group',
    mov_grp_local_cd              string COMMENT 'Local movement type group identifier code',
    mov_grp_local_name            string COMMENT 'Local movement type group name',
    mov_grp_global_id             int    COMMENT 'Global movement type group identifier',
    mov_grp_global_name           string COMMENT 'Global movement type group name',
    movement_channel_id           string COMMENT 'Movement channel identifier',
    campaign_id                   string COMMENT 'Campaign identifier',
    campaign_des                  string COMMENT 'Campaign description',
    seg_local_cd                  string COMMENT 'Local organizational segment identifier code',
    seg_local_name                string COMMENT 'Local organizational segment name',
    seg_global_id                 int    COMMENT 'Global organizational segment identifier',
    seg_global_name               string COMMENT 'Global organizational segment name',
    pre_post_id                   string COMMENT 'Contract type identifier',
    prev_pre_post_id              string COMMENT 'Contract type identifier previous',
    tariff_plan_id                string COMMENT 'Tariff plan ifentifier',
    tariff_plan_des               string COMMENT 'Tariff plan description',
    prev_tariff_plan_id           string COMMENT 'Previous tariff plan ifentifier previous',
    prev_tariff_plan_des          string COMMENT 'Previous tariff plan description',
    prod_type_cd                  string COMMENT 'Local line type identifier code',
    port_op_cd                    string COMMENT 'External operator code for migrations',
    port_op_des                   string COMMENT 'External operator name for migrations'
) COMMENT 'All movements (new adds and churn) by month and MSISDN' 
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
