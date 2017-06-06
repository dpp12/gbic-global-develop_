-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=TRAFFIC_VOICE;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_traffic_voice_ext (
    country_id                   int,
    month_id                     string,
    billing_cycle_dt             string,
    billing_cycle_id             string,
    subscription_id              string,
    msisdn_id                    string,
    day_cd                       int,
    time_range_cd                int,
    imei_num                     bigint,
    call_offnet_fixed_out        int,
    call_onnet_fixed_out         int,
    call_offnet_mobile_out       int,
    call_onnet_mobile_out        int,
    call_international_out       int,
    call_onnet_out_free          int,
    call_onnet_rcm_out           int,
    call_roaming_out             int,
    call_out_special_numbers     int,
    call_fixed_in                int,
    call_offnet_mobile_in        int,
    call_onnet_mobile_in         int,
    call_roaming_in              int,
    call_international_in        int,
    min_offnet_fixed_out         double,
    min_onnet_fixed_out          double,
    min_offnet_mobile_out        double,
    min_onnet_mobile_out         double,
    min_international_out        double,
    min_onnet_free_out           double,
    min_onnet_rcm_out            double,
    min_roaming_out              double,
    min_out_special_numbers      double,
    min_fixed_in                 double,
    min_offnet_mobile_in         double,
    min_onnet_mobile_in          double,
    min_roaming_in               double,
    min_international_in         double,
    min_fixed_out_bundled        double,
    min_mobile_out_bundled       double,
    min_fixed_out_not_bundled    double,
    min_mobile_out_not_bundled   double,
    min_fixed_out_exceed         double,
    min_mobile_out_exceed        double,
    roaming_rv                   double,
    out_other_rv                 double,
    out_national_onnet_rv        double,
    out_national_offnet_rv       double,
    out_national_fixed_rv        double,
    out_international_rv         double,
    call_2g_out                  int,
    call_3g_out                  int,
    call_4g_out                  int,
    call_2g_in                   int,
    call_3g_in                   int,
    call_4g_in                   int,
    min_2g_out                   double,
    min_3g_out                   double,
    min_4g_out                   double,
    min_2g_in                    double,
    min_3g_in                    double,
    min_4g_in                    double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_traffic_voice (
    country_id                   int,
    month_id                     string,
    billing_cycle_dt             string,
    billing_cycle_id             string,
    subscription_id              string,
    msisdn_id                    string,
    day_cd                       int,
    time_range_cd                int,
    imei_num                     bigint,
    call_offnet_fixed_out        int,
    call_onnet_fixed_out         int,
    call_offnet_mobile_out       int,
    call_onnet_mobile_out        int,
    call_international_out       int,
    call_onnet_out_free          int,
    call_onnet_rcm_out           int,
    call_roaming_out             int,
    call_out_special_numbers     int,
    call_fixed_in                int,
    call_offnet_mobile_in        int,
    call_onnet_mobile_in         int,
    call_roaming_in              int,
    call_international_in        int,
    min_offnet_fixed_out         double,
    min_onnet_fixed_out          double,
    min_offnet_mobile_out        double,
    min_onnet_mobile_out         double,
    min_international_out        double,
    min_onnet_free_out           double,
    min_onnet_rcm_out            double,
    min_roaming_out              double,
    min_out_special_numbers      double,
    min_fixed_in                 double,
    min_offnet_mobile_in         double,
    min_onnet_mobile_in          double,
    min_roaming_in               double,
    min_international_in         double,
    min_fixed_out_bundled        double,
    min_mobile_out_bundled       double,
    min_fixed_out_not_bundled    double,
    min_mobile_out_not_bundled   double,
    min_fixed_out_exceed         double,
    min_mobile_out_exceed        double,
    roaming_rv                   double,
    out_other_rv                 double,
    out_national_onnet_rv        double,
    out_national_offnet_rv       double,
    out_national_fixed_rv        double,
    out_international_rv         double,
    call_2g_out                  int,
    call_3g_out                  int,
    call_4g_out                  int,
    call_2g_in                   int,
    call_3g_in                   int,
    call_4g_in                   int,
    min_2g_out                   double,
    min_3g_out                   double,
    min_4g_out                   double,
    min_2g_in                    double,
    min_3g_in                    double,
    min_4g_in                    double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_billing_cycle_for_trafficvoice_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_dim_m_billing_cycle_for_trafficvoice (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_m_lines_for_trafficvoice_ext (
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
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_m_lines_for_trafficvoice (
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
) COMMENT ''
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
ALTER TABLE gbic_dq_traffic_voice_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_traffic_voice_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_m_billing_cycle_for_trafficvoice_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id}
);

ALTER TABLE gbic_dq_dim_m_billing_cycle_for_trafficvoice_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id}
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_M_BILLING_CYCLE';

ALTER TABLE gbic_dq_m_lines_for_trafficvoice_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_m_lines_for_trafficvoice_ext
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
INSERT OVERWRITE TABLE gbic_dq_traffic_voice
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
    call_offnet_fixed_out,
    call_onnet_fixed_out,
    call_offnet_mobile_out,
    call_onnet_mobile_out,
    call_international_out,
    call_onnet_out_free,
    call_onnet_rcm_out,
    call_roaming_out,
    call_out_special_numbers,
    call_fixed_in,
    call_offnet_mobile_in,
    call_onnet_mobile_in,
    call_roaming_in,
    call_international_in,
    min_offnet_fixed_out,
    min_onnet_fixed_out,
    min_offnet_mobile_out,
    min_onnet_mobile_out,
    min_international_out,
    min_onnet_free_out,
    min_onnet_rcm_out,
    min_roaming_out,
    min_out_special_numbers,
    min_fixed_in,
    min_offnet_mobile_in,
    min_onnet_mobile_in,
    min_roaming_in,
    min_international_in,
    min_fixed_out_bundled,
    min_mobile_out_bundled,
    min_fixed_out_not_bundled,
    min_mobile_out_not_bundled,
    min_fixed_out_exceed,
    min_mobile_out_exceed,
    roaming_rv,
    out_other_rv,
    out_national_onnet_rv,
    out_national_offnet_rv,
    out_national_fixed_rv,
    out_international_rv,
    call_2g_out,
    call_3g_out,
    call_4g_out,
    call_2g_in,
    call_3g_in,
    call_4g_in,
    min_2g_out,
    min_3g_out,
    min_4g_out,
    min_2g_in,
    min_3g_in,
    min_4g_in
FROM gbic_dq_traffic_voice_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_m_billing_cycle_for_trafficvoice
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
FROM gbic_dq_dim_m_billing_cycle_for_trafficvoice_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND upper(country_id) != 'COUNTRY_ID';

INSERT OVERWRITE TABLE gbic_dq_m_lines_for_trafficvoice
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
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
FROM gbic_dq_m_lines_for_trafficvoice_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
