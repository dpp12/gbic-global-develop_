-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=IMEI_SALES;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_imei_sales (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    imei_num                      bigint COMMENT 'International Mobile System Equipment Identity',
    pre_post_id                   string COMMENT 'Contract type',
    seg_local_cd                  string COMMENT 'Local organizational segment identifier code',
    seg_local_name                string COMMENT 'Local organizational segment description',
    seg_global_id                 int    COMMENT 'Global organizational segment identifier',
    seg_global_name               string COMMENT 'Global organizational segment description',
    activation_movement           string COMMENT 'Movement detail that causes the activation',
    tariff_plan_id                string COMMENT 'Tariff plan code',
    channel_cd                    string COMMENT 'Sales channel code ',
    sales_network_cd              string COMMENT 'Seles Network code',
    distribution_channel_cd       string COMMENT 'Distribution Channel code',
    campain_cd                    string COMMENT 'Campain code',
    sale_price                    double COMMENT 'Amount of the Price of the sale on the local currency',
    purchase_price                double COMMENT 'Amount of the hadset when it has been purcharsed',
    financial_support             double COMMENT 'Financial support',
    postal_cd                     string COMMENT 'Location of the line ',
    device_name                   string COMMENT 'Additional description of the hadset',
    imei_origin                   string COMMENT 'Handset origin: TEF (sold by Telefonica) - No TEF (unlocked)',
    subscription_id               string COMMENT 'Subscription identifier, encrypted as a String'
) COMMENT 'Handset Sales by month_id, IMEI and MSISDN if it has a line' 
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
