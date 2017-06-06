-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=F_LINES;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_f_lines_ext (
    country_id                   int,
    month_id                     string,
    subscription_id              string,
    administrator_id             string,
    customer_id                  string,
    fix_customer_id              string,
    postal_cd                    string,
    party_type_cd                bigint,
    segment_cd                   string,
    voice_ind                    int,
    voice_activation_dt          string,
    voice_type_cd                string,
    voice_tariff_plan_id         string,
    voice_month_rv               double,
    bband_ind                    int,
    bband_activation_dt          string,
    bband_type_cd                string,
    bband_tariff_plan_id         string,
    speed_band_qt                int,
    bband_month_rv               double,
    tv_ind                       int,
    tv_sales_dt                  string,
    tv_activation_dt             string,
    tv_use_dt                    string,
    tv_promo_id                  int,
    tv_end_promo_dt              string,
    tv_type_cd                   string,
    tv_tariff_plan_id            string,
    tv_points_qt                 int,
    tv_recurring_rv              double,
    tv_non_recurring_rv          double,
    tv_month_rv                  double,
    workstation_ind              int,
    workstation_type_cd          string,
    app_ind                      int,
    total_month_rv               double,
    data_consumed_qt             double,
    calls_voice_qt               double,
    minutes_voice_qt             double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_f_lines (
    country_id                   int,
    month_id                     string,
    subscription_id              string,
    administrator_id             string,
    customer_id                  string,
    fix_customer_id              string,
    postal_cd                    string,
    party_type_cd                bigint,
    segment_cd                   string,
    voice_ind                    int,
    voice_activation_dt          string,
    voice_type_cd                string,
    voice_tariff_plan_id         string,
    voice_month_rv               double,
    bband_ind                    int,
    bband_activation_dt          string,
    bband_type_cd                string,
    bband_tariff_plan_id         string,
    speed_band_qt                int,
    bband_month_rv               double,
    tv_ind                       int,
    tv_sales_dt                  string,
    tv_activation_dt             string,
    tv_use_dt                    string,
    tv_promo_id                  int,
    tv_end_promo_dt              string,
    tv_type_cd                   string,
    tv_tariff_plan_id            string,
    tv_points_qt                 int,
    tv_recurring_rv              double,
    tv_non_recurring_rv          double,
    tv_month_rv                  double,
    workstation_ind              int,
    workstation_type_cd          string,
    app_ind                      int,
    total_month_rv               double,
    data_consumed_qt             double,
    calls_voice_qt               double,
    minutes_voice_qt             double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_flines_ext (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_flines (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_postal_for_flines_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_dim_postal_for_flines (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_f_tariff_plan_for_flines_ext (
    f_country_id                 int,
    f_month_id                   string,
    f_tariff_plan_id             string,
    f_tariff_plan_des            string
) COMMENT 'External table over dim_f_tariff_plan raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_dim_f_tariff_plan_for_flines (
    f_country_id                 int,
    f_month_id                   string,
    f_tariff_plan_id             string,
    f_tariff_plan_des            string
) COMMENT 'ORC table over dim_f_tariff_plan raw files'
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_segments_for_flines_ext (
    seg_month                    string,
    seg_concept_id               int,
    seg_concept_name             string,
    seg_gbic_op_id               int,
    seg_local_cd                 string,
    seg_local_name               string,
    seg_global_id                int,
    seg_global_name              string
) COMMENT ''
PARTITIONED BY (
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_segments_for_flines (
    seg_month                    string,
    seg_concept_id               int,
    seg_concept_name             string,
    seg_gbic_op_id               int,
    seg_local_cd                 string,
    seg_local_name               string,
    seg_global_id                int,
    seg_global_name              string
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
ALTER TABLE gbic_dq_f_lines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_f_lines_ext 
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_customer_for_flines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_customer_for_flines_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/CUSTOMER/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_postal_for_flines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_postal_for_flines_ext
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_POSTAL/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_f_tariff_plan_for_flines_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_f_tariff_plan_for_flines_ext
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_F_TARIFF_PLAN/month=${nominalTime}';

ALTER TABLE gbic_dq_segments_for_flines_ext
DROP IF EXISTS PARTITION (
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_segments_for_flines_ext
ADD PARTITION (
    month = '${nominalTime}'
) LOCATION '{{ cluster.service }}/homog/month=${nominalTime}/dim=1';

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
INSERT OVERWRITE TABLE gbic_dq_f_lines
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    subscription_id,
    administrator_id,
    customer_id,
    fix_customer_id,
    postal_cd,
    party_type_cd,
    segment_cd,
    voice_ind,
    voice_activation_dt,
    voice_type_cd,
    voice_tariff_plan_id,
    voice_month_rv,
    bband_ind,
    bband_activation_dt,
    bband_type_cd,
    bband_tariff_plan_id,
    speed_band_qt,
    bband_month_rv,
    tv_ind,
    tv_sales_dt,
    tv_activation_dt,
    tv_use_dt,
    tv_promo_id,
    tv_end_promo_dt,
    tv_type_cd,
    tv_tariff_plan_id,
    tv_points_qt,
    tv_recurring_rv,
    tv_non_recurring_rv,
    tv_month_rv,
    workstation_ind,
    workstation_type_cd,
    app_ind,
    total_month_rv,
    data_consumed_qt,
    calls_voice_qt,
    minutes_voice_qt
FROM gbic_dq_f_lines_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_customer_for_flines
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
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
FROM gbic_dq_customer_for_flines_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_postal_for_flines
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    postal_id,
    location_level,
    location_name
FROM gbic_dq_dim_postal_for_flines_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_f_tariff_plan_for_flines
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    f_country_id,
    f_month_id,
    f_tariff_plan_id,
    f_tariff_plan_des
FROM gbic_dq_dim_f_tariff_plan_for_flines_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(f_month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_segments_for_flines
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
FROM gbic_dq_segments_for_flines_ext
WHERE month = '${nominalTime}'
  AND seg_gbic_op_id = ${gbic_op_id};
