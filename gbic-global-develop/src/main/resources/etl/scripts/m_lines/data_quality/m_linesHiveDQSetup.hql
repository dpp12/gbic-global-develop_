-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=M_LINES;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_m_lines_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_m_lines (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_postal_for_mlines_ext (
    country_id                   int,
    month_id                     string,
    postal_id                    string,
    location_level               string,
    location_name                string
) COMMENT 'External table over dim_postal raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_dim_postal_for_mlines (
    country_id                   int,
    month_id                     string,
    postal_id                    string,
    location_level               string,
    location_name                string
) COMMENT 'ORC table over dim_postal raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_segments_for_mlines_ext (
    seg_month                    string,
    seg_concept_id               int,
    seg_concept_name             string,
    seg_gbic_op_id               int,
    seg_local_cd                 string,
    seg_local_name               string,
    seg_global_id                int,
    seg_global_name              string
) COMMENT 'External table over segments raw files'
PARTITIONED BY (
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_segments_for_mlines (
    seg_month        string,
    seg_concept_id   int,
    seg_concept_name string,
    seg_gbic_op_id   int,
    seg_local_cd     string,
    seg_local_name   string,
    seg_global_id    int,
    seg_global_name  string
) COMMENT 'ORC table over segments raw files'
PARTITIONED BY (
    gbic_op_id       int,
    month            string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_tacs_for_mlines_ext (
    tac                string,
    volume             bigint,
    technology_4g_sp   string,
    technology_4g_br   string,
    technology_4g_mx   string,
    technology_4g_ch   string,
    technology_4g_ur   string,
    technology_4g_pe   string,
    technology_4g_ar   string,
    des_manufact       string,
    des_brand          string,
    des_model          string,
    market_category    string,
    tef_category       string,
    touchscreen        string,
    keyboard           string,
    os                 string,
    version_os         string,
    technology_2g      string,
    technology_3g      string,
    technology_4g_dl   string,
    technology_4g_ul   string
) COMMENT 'External table over tacs raw files'
PARTITIONED BY (
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_tacs_for_mlines (
    tac                string,
    volume             bigint,
    technology_4g_sp   string,
    technology_4g_br   string,
    technology_4g_mx   string,
    technology_4g_ch   string,
    technology_4g_ur   string,
    technology_4g_pe   string,
    technology_4g_ar   string,
    des_manufact       string,
    des_brand          string,
    des_model          string,
    market_category    string,
    tef_category       string,
    touchscreen        string,
    keyboard           string,
    os                 string,
    version_os         string,
    technology_2g      string,
    technology_3g      string,
    technology_4g_dl   string,
    technology_4g_ul   string
) COMMENT 'ORC table over tacs raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_tariff_plan_for_mlines_ext (
    country_id                   string,
    month_id                     string,
    tariff_plan_id               string,
    des_plan                     string,
    data_tariff_ind              string
) COMMENT 'External table over tacs raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_dim_m_tariff_plan_for_mlines (
    country_id                   string,
    month_id                     string,
    tariff_plan_id               string,
    des_plan                     string,
    data_tariff_ind              string
) COMMENT 'ORC table over tacs raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_mlines_ext (
    country_id                   int,
    month_id                     string,
    ob_id                        string,
    party_type_cd                int,
    party_identification_type_cd string,
    party_identification_num     string,
    customer_id                  string,
    activation_dt                string,
    segment_cd                   string,
    birth_dt                     string,
    age_id                       int,
    gender_type_cd               string,
    org_name                     string,
    cust_education               string,
    cust_prod_qt                 string,
    cust_life_cycle              string,
    socioeconomic_level          string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_customer_for_mlines (
    country_id                   int,
    month_id                     string,
    ob_id                        string,
    party_type_cd                int,
    party_identification_type_cd string,
    party_identification_num     string,
    customer_id                  string,
    activation_dt                string,
    segment_cd                   string,
    birth_dt                     string,
    age_id                       int,
    gender_type_cd               string,
    org_name                     string,
    cust_education               string,
    cust_prod_qt                 string,
    cust_life_cycle              string,
    socioeconomic_level          string
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
ALTER TABLE gbic_dq_m_lines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_m_lines_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_postal_for_mlines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_postal_for_mlines_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_POSTAL/month=${nominalTime}';

ALTER TABLE gbic_dq_segments_for_mlines_ext
DROP IF EXISTS PARTITION (
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_segments_for_mlines_ext
ADD PARTITION (
    month = '${nominalTime}'
) LOCATION '{{ cluster.service }}/homog/month=${nominalTime}/dim=1';

ALTER TABLE gbic_dq_tacs_for_mlines_ext
DROP IF EXISTS PARTITION (
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_tacs_for_mlines_ext
ADD PARTITION (
    month = '${nominalTime}'
) LOCATION '{{ cluster.common }}/tacs/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_m_tariff_plan_for_mlines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_tariff_plan_for_mlines_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_M_TARIFF_PLAN/month=${nominalTime}';

ALTER TABLE gbic_dq_customer_for_mlines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
);

ALTER TABLE gbic_dq_customer_for_mlines_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/CUSTOMER/month=${nominalTime}';


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
INSERT OVERWRITE TABLE gbic_dq_m_lines PARTITION (
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
    gbic_dq_m_lines_ext
WHERE 
    gbic_op_id = ${gbic_op_id}    AND
    month = '${nominalTime}' AND
    upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_postal_for_mlines PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    postal_id,
    location_level,
    location_name
FROM gbic_dq_dim_postal_for_mlines_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_segments_for_mlines
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT 
    seg_month,
    seg_concept_id,
    seg_concept_name,
    seg_gbic_op_id,
    seg_local_cd,
    seg_local_name,
    seg_global_id,
    seg_global_name
FROM gbic_dq_segments_for_mlines_ext
WHERE month = '${nominalTime}'
  AND seg_gbic_op_id = ${gbic_op_id};

INSERT OVERWRITE TABLE gbic_dq_tacs_for_mlines PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    tac,
    volume,
    technology_4g_sp,
    technology_4g_br,
    technology_4g_mx,
    technology_4g_ch,
    technology_4g_ur,
    technology_4g_pe,
    technology_4g_ar,
    des_manufact,
    des_brand,
    des_model,
    market_category,
    tef_category,
    touchscreen,
    keyboard,
    os,
    version_os,
    technology_2g,
    technology_3g,
    technology_4g_dl,
    technology_4g_ul
FROM gbic_dq_tacs_for_mlines_ext
WHERE month = '${nominalTime}'
  AND upper(tac) != 'TAC';

INSERT OVERWRITE TABLE gbic_dq_dim_m_tariff_plan_for_mlines PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    tariff_plan_id,
    des_plan,
    data_tariff_ind
FROM gbic_dq_dim_m_tariff_plan_for_mlines_ext
WHERE month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_customer_for_mlines PARTITION (
    gbic_op_id=${gbic_op_id},
    month='${nominalTime}'
) SELECT
    country_id,
    month_id,
    ob_id,
    party_type_cd,
    party_identification_type_cd,
    party_identification_num,
    customer_id,
    activation_dt,
    segment_cd,
    birth_dt,
    age_id,
    gender_type_cd,
    org_name,
    cust_education,
    cust_prod_qt,
    cust_life_cycle,
    socioeconomic_level
FROM gbic_dq_customer_for_mlines_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
