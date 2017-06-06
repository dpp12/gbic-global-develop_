-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=IMEI_SALES;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_imei_sales_ext (
    country_id              int,
    month_id                string,
    msisdn_id               string,
    imei_num                bigint,
    pre_post_id             string,
    segment_cd              string,
    activation_movement     string,
    tariff_plan_id          string,
    channel_cd              string,
    sales_network_cd        string,
    distribution_channel_cd string,
    campain_cd              string,
    sale_price              double,
    purchase_price          double,
    financial_support       double,
    postal_cd               string,
    device_name             string,
    imei_origin             string,
    subscription_id         string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id              int,
    month                   string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_imei_sales (
    country_id              int,
    month_id                string,
    msisdn_id               string,
    imei_num                bigint,
    pre_post_id             string,
    segment_cd              string,
    activation_movement     string,
    tariff_plan_id          string,
    channel_cd              string,
    sales_network_cd        string,
    distribution_channel_cd string,
    campain_cd              string,
    sale_price              double,
    purchase_price          double,
    financial_support       double,
    postal_cd               string,
    device_name             string,
    imei_origin             string,
    subscription_id         string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id              int,
    month                   string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_postal_for_imeisales_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_dim_postal_for_imeisales (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_segments_for_imeisales_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_segments_for_imeisales (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_tariff_plan_for_imeisales_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_dim_m_tariff_plan_for_imeisales (
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



-------------------------------------------------------------------------------
-- Reference received files as external partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_imei_sales_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_imei_sales_ext
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_postal_for_imeisales_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_postal_for_imeisales_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_POSTAL/month=${nominalTime}';

ALTER TABLE gbic_dq_segments_for_imeisales_ext
DROP IF EXISTS PARTITION (
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_segments_for_imeisales_ext
ADD PARTITION (
    month = '${nominalTime}'
) LOCATION '{{ cluster.service }}/homog/month=${nominalTime}/dim=1';


ALTER TABLE gbic_dq_dim_m_tariff_plan_for_imeisales_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_tariff_plan_for_imeisales_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month      = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_M_TARIFF_PLAN/month=${nominalTime}';


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
INSERT OVERWRITE TABLE gbic_dq_imei_sales
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    msisdn_id,
    imei_num,
    pre_post_id,
    segment_cd,
    activation_movement,
    tariff_plan_id,
    channel_cd,
    sales_network_cd,
    distribution_channel_cd,
    campain_cd,
    sale_price,
    purchase_price,
    financial_support,
    postal_cd,
    device_name,
    imei_origin,
    subscription_id
FROM  gbic_dq_imei_sales_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_postal_for_imeisales PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    postal_id,
    location_level,
    location_name
FROM gbic_dq_dim_postal_for_imeisales_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_segments_for_imeisales
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
FROM gbic_dq_segments_for_imeisales_ext
WHERE month = '${nominalTime}'
  AND seg_gbic_op_id = ${gbic_op_id};

INSERT OVERWRITE TABLE gbic_dq_dim_m_tariff_plan_for_imeisales PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    tariff_plan_id,
    des_plan,
    data_tariff_ind
FROM gbic_dq_dim_m_tariff_plan_for_imeisales_ext
WHERE month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
