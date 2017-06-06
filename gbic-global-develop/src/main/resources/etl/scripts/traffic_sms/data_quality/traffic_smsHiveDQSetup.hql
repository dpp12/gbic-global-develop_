-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=TRAFFIC_SMS;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ interface }};

SET hive.mapred.supports.subdirectories=TRUE;
SET mapred.input.dir.recursive=TRUE;

-------------------------------------------------------------------------------
-- DATA QUALITY AREA
-------------------------------------------------------------------------------
-- DDL for {{ interface }} quality tables
--     * External table: gbic_dq_{{ interface }}_ext
--     * Consolidation table: gbic_dq_{{ interface }}
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global_dq;

USE {{ project.prefix }}gbic_global_dq;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_traffic_sms_ext(
    country_id                   int,
    month_id                     string,
    billing_cycle_dt             string,
    billing_cycle_id             string,
    subscription_id              string,
    msisdn_id                    string,
    day_cd                       int,
    time_range_cd                int,
    imei_num                     bigint,
    sms_offnet_out_qt            int,
    sms_onnet_out_qt             int,
    sms_international_out_qt     int,
    sms_roaming_out_qt           int,
    sms_offnet_in_qt             int,
    sms_onnet_in_qt              int,
    sms_international_in_qt      int,
    sms_roaming_in_qt            int,
    sms_out_bundled_rv           double,
    sms_out_not_bundled_rv       double,
    sms_roaming_out_rv           double
) COMMENT 'External table over traffic_sms raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_traffic_sms(
    country_id                   int,
    month_id                     string,
    billing_cycle_dt             string,
    billing_cycle_id             string,
    subscription_id              string,
    msisdn_id                    string,
    day_cd                       int,
    time_range_cd                int,
    imei_num                     bigint,
    sms_offnet_out_qt            int,
    sms_onnet_out_qt             int,
    sms_international_out_qt     int,
    sms_roaming_out_qt           int,
    sms_offnet_in_qt             int,
    sms_onnet_in_qt              int,
    sms_international_in_qt      int,
    sms_roaming_in_qt            int,
    sms_out_bundled_rv           double,
    sms_out_not_bundled_rv       double,
    sms_roaming_out_rv           double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_billing_cycle_for_trafficsms_ext (
    country_id                   string,
    billing_cycle_month          string,
    billing_cycle_id             string,
    billing_cycle_des            string,
    billing_cycle_start_dt       string,
    billing_cycle_end_dt         string,
    billing_due_dt               string,
    billing_rv_computes          string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_dim_m_billing_cycle_for_trafficsms (
    country_id                   string,
    billing_cycle_month          string,
    billing_cycle_id             string,
    billing_cycle_des            string,
    billing_cycle_start_dt       string,
    billing_cycle_end_dt         string,
    billing_due_dt               string,
    billing_rv_computes          string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_m_lines_for_trafficsms_ext (
    country_id                   string,
    month_id                     string,
    msisdn_id                    string,
    subscription_id              string,
    imsi_id                      string,
    customer_id                  string,
    mobile_customer_id           string,
    party_type_cd                string,
    activation_dt                string,
    prod_type_cd                 string,
    imei_num                     string,
    line_status_cd               string,
    segment_cd                   string,
    pre_post_id                  string,
    account_id                   string,
    tariff_plan_id               string,
    billing_cycle_id             string,
    postal_cd                    string,
    multisim_ind                 string,
    exceed_ind                   string,
    data_tariff_ind              string,
    extra_data_num               string,
    extra_data_rv                string,
    extra_data_qt                string,
    ppu_num                      string,
    ppu_rv                       string,
    ppu_qt                       string,
    data_consumed_qt             string,
    data_bundled_qt              string,
    call_voice_qt                string,
    voice_consumed_qt            string,
    sms_consumed_qt              string,
    prepaid_top_up_id            string,
    top_up_cost_num              string,
    top_up_cost_rv               string,
    top_up_promo_num             string,
    top_up_promo_rv              string,
    no_top_up_rv                 string,
    total_rv                     string,
    bta_ind                      string
) COMMENT 'External table over dim_m_group_sva raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_m_lines_for_trafficsms (
    country_id                   string,
    month_id                     string,
    msisdn_id                    string,
    subscription_id              string,
    imsi_id                      string,
    customer_id                  string,
    mobile_customer_id           string,
    party_type_cd                string,
    activation_dt                string,
    prod_type_cd                 string,
    imei_num                     string,
    line_status_cd               string,
    segment_cd                   string,
    pre_post_id                  string,
    account_id                   string,
    tariff_plan_id               string,
    billing_cycle_id             string,
    postal_cd                    string,
    multisim_ind                 string,
    exceed_ind                   string,
    data_tariff_ind              string,
    extra_data_num               string,
    extra_data_rv                string,
    extra_data_qt                string,
    ppu_num                      string,
    ppu_rv                       string,
    ppu_qt                       string,
    data_consumed_qt             string,
    data_bundled_qt              string,
    call_voice_qt                string,
    voice_consumed_qt            string,
    sms_consumed_qt              string,
    prepaid_top_up_id            string,
    top_up_cost_num              string,
    top_up_cost_rv               string,
    top_up_promo_num             string,
    top_up_promo_rv              string,
    no_top_up_rv                 string,
    total_rv                     string,
    bta_ind                      string
) COMMENT 'ORC table over dim_m_group_sva raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");


-------------------------------------------------------------------------------
-- Reference received files as external partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_traffic_sms_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_traffic_sms_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_m_billing_cycle_for_trafficsms_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id}
);

ALTER TABLE gbic_dq_dim_m_billing_cycle_for_trafficsms_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id}
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_M_BILLING_CYCLE';

ALTER TABLE gbic_dq_m_lines_for_trafficsms_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_m_lines_for_trafficsms_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/M_LINES/month=${nominalTime}';

-------------------------------------------------------------------------------
-- Reference service's quality files as base for checking received data
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_service_checks
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
);

ALTER TABLE gbic_dq_service_checks
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
) LOCATION '{{ hdfs.srvchecks }}/${ob}/${fileName}/day=${nominalTime}';

-------------------------------------------------------------------------------
-- Reference platform's quality criterias for the tests applied to the file
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_file_test
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
);

ALTER TABLE gbic_dq_file_test
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
) LOCATION '{{ hdfs.filetests }}/${ob}/${fileName}/day=${nominalTime}';

-------------------------------------------------------------------------------
-- Populate consolidation table
-------------------------------------------------------------------------------
INSERT OVERWRITE TABLE gbic_dq_traffic_sms
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    billing_cycle_dt,
    billing_cycle_id,
    subscription_id,
    msisdn_id,
    day_cd,
    time_range_cd,
    imei_num,
    sms_offnet_out_qt,
    sms_onnet_out_qt,
    sms_international_out_qt,
    sms_roaming_out_qt,
    sms_offnet_in_qt,
    sms_onnet_in_qt,
    sms_international_in_qt,
    sms_roaming_in_qt,
    sms_out_bundled_rv,
    sms_out_not_bundled_rv,
    sms_roaming_out_rv
FROM gbic_dq_traffic_sms_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_m_billing_cycle_for_trafficsms
PARTITION (
    gbic_op_id = ${gbic_op_id}
) SELECT
    country_id,
    billing_cycle_month,
    billing_cycle_id,
    billing_cycle_des,
    billing_cycle_start_dt,
    billing_cycle_end_dt,
    billing_due_dt,
    billing_rv_computes
FROM gbic_dq_dim_m_billing_cycle_for_trafficsms_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND upper(country_id) != 'COUNTRY_ID';

INSERT OVERWRITE TABLE gbic_dq_m_lines_for_trafficsms
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    msisdn_id,
    subscription_id,
    imsi_id,
    customer_id,
    mobile_customer_id,
    party_type_cd,
    activation_dt,
    prod_type_cd,
    imei_num,
    line_status_cd,
    segment_cd,
    pre_post_id,
    account_id,
    tariff_plan_id,
    billing_cycle_id,
    postal_cd,
    multisim_ind,
    exceed_ind,
    data_tariff_ind,
    extra_data_num,
    extra_data_rv,
    extra_data_qt,
    ppu_num,
    ppu_rv,
    ppu_qt,
    data_consumed_qt,
    data_bundled_qt,
    call_voice_qt,
    voice_consumed_qt,
    sms_consumed_qt,
    prepaid_top_up_id,
    top_up_cost_num,
    top_up_cost_rv,
    top_up_promo_num,
    top_up_promo_rv,
    no_top_up_rv,
    total_rv,
    bta_ind
FROM gbic_dq_m_lines_for_trafficsms_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
