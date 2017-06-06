-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=HANDSET_UPGRADE;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_handset_upgrade (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    activation_dt                 string COMMENT 'Activation date for the line',
    imei_num                      bigint COMMENT 'International Mobile System Equipment Identity',
    hu_dt                         string COMMENT 'Day of Redemption',
    hu_type_id                    string COMMENT 'Type of Redemption',
    hu_amount                     double COMMENT 'Price of the redemption',
    commission_amount             int    COMMENT 'Distributor commission amount',
    financial_support             int    COMMENT 'Financial support',
    hu_loyalty_points_balance     int    COMMENT 'Total of loyalty plan points available',
    hu_loyalty_points_redeemed    int    COMMENT 'Total of loyalty plan points redeemed '
) COMMENT 'Handset changes by month, IMEI and MSISDN' 
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
