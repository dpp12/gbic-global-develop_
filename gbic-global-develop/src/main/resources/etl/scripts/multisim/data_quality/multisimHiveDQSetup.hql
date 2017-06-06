-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=MULTISIM;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_multisim_ext (
    country_id                   int,
    month_id                     string,
    msisdn_main                  string,
    msisdn_add                   string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_multisim (
    country_id                   int,
    month_id                     string,
    msisdn_main                  string,
    msisdn_add                   string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_m_lines_for_multisim_ext (
    country_id                   int,
    month_id                     string,
    msisdn_id                    string,
    subscription_id              string,
    imsi_id                      string,
    customer_id                  string,
    mobile_customer_id           string,
    party_type_cd                bigint,
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
    multisim_ind                 bigint,
    exceed_ind                   bigint,
    data_tariff_ind              bigint,
    extra_data_num               bigint,
    extra_data_rv                double,
    extra_data_qt                bigint,
    ppu_num                      bigint,
    ppu_rv                       double,
    ppu_qt                       bigint,
    data_consumed_qt             double,
    data_bundled_qt              double,
    call_voice_qt                bigint,
    voice_consumed_qt            double,
    sms_consumed_qt              bigint,
    prepaid_top_up_id            bigint,
    top_up_cost_num              bigint,
    top_up_cost_rv               double,
    top_up_promo_num             bigint,
    top_up_promo_rv              double,
    no_top_up_rv                 double,
    total_rv                     double,
    bta_ind                      int
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_m_lines_for_multisim (
    country_id                   int,
    month_id                     string,
    msisdn_id                    string,
    subscription_id              string,
    imsi_id                      string,
    customer_id                  string,
    mobile_customer_id           string,
    party_type_cd                bigint,
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
    multisim_ind                 bigint,
    exceed_ind                   bigint,
    data_tariff_ind              bigint,
    extra_data_num               bigint,
    extra_data_rv                double,
    extra_data_qt                bigint,
    ppu_num                      bigint,
    ppu_rv                       double,
    ppu_qt                       bigint,
    data_consumed_qt             double,
    data_bundled_qt              double,
    call_voice_qt                bigint,
    voice_consumed_qt            double,
    sms_consumed_qt              bigint,
    prepaid_top_up_id            bigint,
    top_up_cost_num              bigint,
    top_up_cost_rv               double,
    top_up_promo_num             bigint,
    top_up_promo_rv              double,
    no_top_up_rv                 double,
    total_rv                     double,
    bta_ind                      int
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
ALTER TABLE gbic_dq_multisim_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_multisim_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_m_lines_for_multisim_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_m_lines_for_multisim_ext
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
INSERT OVERWRITE TABLE gbic_dq_multisim
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    msisdn_main,
    msisdn_add
FROM gbic_dq_multisim_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_m_lines_for_multisim PARTITION (
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
FROM 
    gbic_dq_m_lines_for_multisim_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
