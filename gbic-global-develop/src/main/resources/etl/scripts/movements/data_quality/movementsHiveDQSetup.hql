-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=MOVEMENTS;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_movements_ext (
    country_id                   int,
    month_id                     string,
    customer_id                  string,
    msisdn_id                    string,
    subscription_id              string,
    activation_dt                string,
    movement_id                  string,
    movement_dt                  string,
    movement_channel_id          string,
    campaign_id                  string,
    segment_cd                   string,
    pre_post_id                  string,
    prev_pre_post_id             string,
    tariff_plan_id               string,
    prev_tariff_plan_id          string,
    prod_type_cd                 string,
    port_op_cd                   string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_movements (
    country_id                   int,
    month_id                     string,
    customer_id                  string,
    msisdn_id                    string,
    subscription_id              string,
    activation_dt                string,
    movement_id                  string,
    movement_dt                  string,
    movement_channel_id          string,
    campaign_id                  string,
    segment_cd                   string,
    pre_post_id                  string,
    prev_pre_post_id             string,
    tariff_plan_id               string,
    prev_tariff_plan_id          string,
    prod_type_cd                 string,
    port_op_cd                   string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_movements_ext (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_movements (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_movement_for_movements_ext (
    country_id                   int,
    month_id                     string,
    movement_id                  string,
    movement_des                 string,
    count_movement_qt            int,
    group_movement_cd            string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_movement_for_movements (
    country_id                   int,
    month_id                     string,
    movement_id                  string,
    movement_des                 string,
    count_movement_qt            int,
    group_movement_cd            string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_tariff_plan_for_movements_ext (
     country_id                  int,
     month_id                    string,
     tariff_plan_id              string,
     des_plan                    string,
     data_tariff_ind             int
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_tariff_plan_for_movements (
    country_id                   int,
    month_id                     string,
    tariff_plan_id               string,
    des_plan                     string,
    data_tariff_ind              int
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_segments_for_movements_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_segments_for_movements (
    seg_month                    string,
    seg_concept_id               int,
    seg_concept_name             string,
    seg_gbic_op_id               int,
    seg_local_cd                 string,
    seg_local_name               string,
    seg_global_id                int,
    seg_global_name              string
) COMMENT 'ORC table over segments raw files'
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
ALTER TABLE gbic_dq_movements_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_movements_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_segments_for_movements_ext
DROP IF EXISTS PARTITION (
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_segments_for_movements_ext
ADD PARTITION (
    month = '${nominalTime}'
) LOCATION '{{ cluster.service }}/homog/month=${nominalTime}/dim=1';

ALTER TABLE gbic_dq_customer_for_movements_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_customer_for_movements_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/CUSTOMER/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_m_movement_for_movements_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_movement_for_movements_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_M_MOVEMENT/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_m_tariff_plan_for_movements_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_tariff_plan_for_movements_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
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
INSERT OVERWRITE TABLE gbic_dq_movements
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    customer_id,
    msisdn_id,
    subscription_id,
    activation_dt,
    movement_id,
    movement_dt,
    movement_channel_id,
    campaign_id,
    segment_cd,
    pre_post_id,
    prev_pre_post_id,
    tariff_plan_id,
    prev_tariff_plan_id,
    prod_type_cd,
    port_op_cd
FROM gbic_dq_movements_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_customer_for_movements
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
FROM gbic_dq_customer_for_movements_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_m_movement_for_movements
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    movement_id,
    movement_des,
    count_movement_qt,
    group_movement_cd
FROM gbic_dq_dim_m_movement_for_movements_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_m_tariff_plan_for_movements
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    tariff_plan_id,
    des_plan,
    data_tariff_ind
FROM gbic_dq_dim_m_tariff_plan_for_movements_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_segments_for_movements
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
FROM gbic_dq_segments_for_movements_ext
WHERE month = '${nominalTime}'
  AND seg_gbic_op_id = ${gbic_op_id};
