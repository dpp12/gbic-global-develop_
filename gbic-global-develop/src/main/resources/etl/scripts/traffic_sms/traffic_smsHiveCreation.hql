-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=TRAFFIC_SMS;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_traffic_sms (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    billing_cycle_id              string COMMENT 'Billing cycle code',
    billing_cycle_des             string COMMENT 'Billing cycle description',
    billing_cycle_start_dt        string COMMENT 'Billing cycle start date',
    billing_cycle_end_dt          string COMMENT 'Billing cycle end date',
    billing_due_dt                string COMMENT 'Billing cycle due date',
    billing_rv_computes           int    COMMENT 'When revenue computes: 0 Same month, 1 Next month, 2 In two months, -1 Previous Month',
    subscription_id               string COMMENT 'Subscription identifier, encrypted as a String',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    day_cd                        int    COMMENT 'Type of day the call was made',
    time_range_cd                 int    COMMENT 'Time range call started',
    imei_num                      bigint COMMENT 'International Mobile System Equipment Identity',
    sms_offnet_out_qt             int    COMMENT 'Outgoing  SMS Off Net',
    sms_onnet_out_qt              int    COMMENT 'Outgoing SMS On Net ',
    sms_international_out_qt      int    COMMENT 'International Outgoing SMS',
    sms_roaming_out_qt            int    COMMENT 'Outgoing Roaming SMS ',
    sms_offnet_in_qt              int    COMMENT 'Ingoing Off Net SMS',
    sms_onnet_in_qt               int    COMMENT 'Ingoing On Net SMS',
    sms_international_in_qt       int    COMMENT 'International Ingoing SMS',
    sms_roaming_in_qt             int    COMMENT 'Ingoing Roaming SMS ',
    sms_out_bundled_rv            double COMMENT 'Total Outgoing SMS Revenue included in the Bundle',
    sms_out_not_bundled_rv        double COMMENT 'Total Outgoing SMS Revenue nor included in the BundleE',
    sms_roaming_out_rv            double COMMENT 'Outgoing Roaming SMS Revenue'
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
