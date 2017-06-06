-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=DAILY_TRAFFIC;
-- SET hivevar:nominalTime=2015-08-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_daily_traffic (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    call_dt                       string COMMENT 'Date of the call',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    imei_num                      bigint COMMENT 'International Mobile System Equipment Identity',
    day_type_cd                   string COMMENT 'Day of the call',
    bank_holiday_cd               int    COMMENT 'Bank holiday identifier',
    roaming_cd                    int    COMMENT 'Roaming indicator',
    calls_total_qt                int    COMMENT 'Total number of calls',
    minutes_tot_qt                double COMMENT 'Total duration (minutes)',
    sms_total_qt                  int    COMMENT 'Total number of messages',
    mb_total_qt                   double COMMENT 'Total mb consumed',
    subscription_id               string COMMENT 'Subscription identifier'
) COMMENT 'Keeps the total number of calls for IMEIS at daily level in the month and MSISDN' 
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
