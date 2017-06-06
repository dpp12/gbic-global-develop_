-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=TRAFFIC_DATA;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_traffic_data (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    billing_cycle_id              string COMMENT 'Billing cycle identifier',
    billing_cycle_des             string COMMENT 'Billing cycle description',
    billing_cycle_start_dt        string COMMENT 'Billing cycle start date',
    billing_cycle_end_dt          string COMMENT 'Billing cycle end date',
    billing_due_dt                string COMMENT 'Billing cycle due date',
    billing_rv_computes           int    COMMENT 'When revenue computes: 0 Same month, 1 Next month, 2 In two months, -1 Previous Month',
    subscription_id               string COMMENT 'Subscription identifier, encrypted as a String',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    day_cd                        int    COMMENT 'Day type: Mon to Fri or Sat, Sun or Bank Holiday',
    time_range_cd                 int    COMMENT 'Time range call started. These rage are able to change in the future',
    imei_num                      bigint COMMENT 'International Mobile System Equipment Identity',
    total_qt                      double COMMENT 'Mb of data Quantity',
    mb_2g_qt                      double COMMENT 'Mb of data Quantity  in 2G net',
    mb_3g_qt                      double COMMENT 'Mb of data Quantity  in 3G net',
    mb_4g_qt                      double COMMENT 'Mb of data Quantity  in 4G net',
    mb_roaming                    double COMMENT 'Mb of data Quantity  in Roaming'
) COMMENT 'All IMEIS by month and MSISDN' 
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
