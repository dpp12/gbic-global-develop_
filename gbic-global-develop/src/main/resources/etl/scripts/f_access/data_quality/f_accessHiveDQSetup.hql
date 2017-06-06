-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=F_ACCESS;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ interface }};

-------------------------------------------------------------------------------
-- DATA QUALITY AREA
-------------------------------------------------------------------------------
-- DDL for {{ interface }} quality tables
--     * External table: gbic_dq_{{ interface }}_ext
--     * Consolidation table: gbic_dq_{{ interface }}
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global_dq;

USE {{ project.prefix }}gbic_global_dq;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_f_access_ext (
    country_id                   int,
    month_id                     string,
    customer_id                  string,
    segment_cd                   string,
    service_cd                   string,
    technology_type              string,
    access_qt                    int
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_f_access (
    country_id                   int,
    month_id                     string,
    customer_id                  string,
    segment_cd                   string,
    service_cd                   string,
    technology_type              string,
    access_qt                    int
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_faccess_ext (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_faccess (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_f_lines_for_faccess_ext (
    country_id           int,
    month_id             string,
    subscription_id      string,
    administrator_id     string,
    customer_id          string,
    fix_customer_id      string,
    postal_cd            string,
    party_type_cd        bigint,
    segment_cd           string,
    voice_ind            int,
    voice_activation_dt  string,
    voice_type_cd        string,
    voice_tariff_plan_id string,
    voice_month_rv       double,
    bband_ind            int,
    bband_activation_dt  string,
    bband_type_cd        string,
    bband_tariff_plan_id string,
    speed_band_qt        int,
    bband_month_rv       double,
    tv_ind               int,
    tv_sales_dt          string,
    tv_activation_dt     string,
    tv_use_dt            string,
    tv_promo_id          int,
    tv_end_promo_dt      string,
    tv_type_cd           string,
    tv_tariff_plan_id    string,
    tv_points_qt         int,
    tv_recurring_rv      double,
    tv_non_recurring_rv  double,
    tv_month_rv          double,
    workstation_ind      int,
    workstation_type_cd  string,
    app_ind              int,
    total_month_rv       double,
    data_consumed_qt     double,
    calls_voice_qt       double,
    minutes_voice_qt     double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_f_lines_for_faccess (
    country_id           int,
    month_id             string,
    subscription_id      string,
    administrator_id     string,
    customer_id          string,
    fix_customer_id      string,
    postal_cd            string,
    party_type_cd        bigint,
    segment_cd           string,
    voice_ind            int,
    voice_activation_dt  string,
    voice_type_cd        string,
    voice_tariff_plan_id string,
    voice_month_rv       double,
    bband_ind            int,
    bband_activation_dt  string,
    bband_type_cd        string,
    bband_tariff_plan_id string,
    speed_band_qt        int,
    bband_month_rv       double,
    tv_ind               int,
    tv_sales_dt          string,
    tv_activation_dt     string,
    tv_use_dt            string,
    tv_promo_id          int,
    tv_end_promo_dt      string,
    tv_type_cd           string,
    tv_tariff_plan_id    string,
    tv_points_qt         int,
    tv_recurring_rv      double,
    tv_non_recurring_rv  double,
    tv_month_rv          double,
    workstation_ind      int,
    workstation_type_cd  string,
    app_ind              int,
    total_month_rv       double,
    data_consumed_qt     double,
    calls_voice_qt       double,
    minutes_voice_qt     double
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
ALTER TABLE gbic_dq_f_access_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_f_access_ext 
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_customer_for_faccess_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_customer_for_faccess_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/CUSTOMER/month=${nominalTime}';

ALTER TABLE gbic_dq_f_lines_for_faccess_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
);

ALTER TABLE gbic_dq_f_lines_for_faccess_ext
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/F_LINES/month=${nominalTime}';

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
INSERT OVERWRITE TABLE gbic_dq_f_access
PARTITION ( gbic_op_id=${gbic_op_id}, month='${nominalTime}' )
SELECT country_id,
       month_id,
       customer_id,
       segment_cd,
       service_cd,
       technology_type,
       access_qt
FROM gbic_dq_f_access_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_customer_for_faccess
PARTITION ( gbic_op_id=${gbic_op_id}, month='${nominalTime}' )
SELECT country_id,
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
FROM gbic_dq_customer_for_faccess_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_f_lines_for_faccess
PARTITION ( gbic_op_id=${gbic_op_id}, month='${nominalTime}' )
SELECT country_id,
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
FROM gbic_dq_f_lines_for_faccess_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
