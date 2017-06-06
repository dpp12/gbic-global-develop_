-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=bra;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=F_TARIFF_PLAN;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=201;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_f_tariff_plan_ext (
    country_id                   int,
    month_id                     string,
    subscription_id              string,
    tariff_plan_id               string,
    tariff_plan_des              string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_f_tariff_plan (
    country_id                   int,
    month_id                     string,
    subscription_id              string,
    tariff_plan_id               string,
    tariff_plan_des              string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_f_lines_for_ftariffplan_ext (
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
    data_consumed_qt             double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_f_lines_for_ftariffplan (
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
    data_consumed_qt             double
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
ALTER TABLE gbic_dq_f_tariff_plan_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_f_tariff_plan_ext 
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_f_lines_for_ftariffplan_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_f_lines_for_ftariffplan_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
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
INSERT OVERWRITE TABLE gbic_dq_f_tariff_plan
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month='${nominalTime}'
) SELECT
    country_id,
    month_id,
    subscription_id,
    tariff_plan_id,
    tariff_plan_des
FROM gbic_dq_f_tariff_plan_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_f_lines_for_ftariffplan
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
    data_consumed_qt
FROM gbic_dq_f_lines_for_ftariffplan_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
