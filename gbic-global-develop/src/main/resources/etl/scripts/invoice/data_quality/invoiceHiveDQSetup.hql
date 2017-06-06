-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=INVOICE;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_invoice_ext (
    country_id                   int,
    month_id                     string,
    customer_id                  string,
    msisdn_id                    string,
    activation_dt                string,
    billing_cycle_id             string,
    quota_data_rv                double,
    quota_voice_rv               double,
    quota_mess_rv                double,
    quota_agg_rv                 double,
    traffic_data_rv              double,
    traffic_voice_rv             double,
    traffic_mess_rv              double,
    traffic_agg_rv               double,
    roaming_rv                   double,
    sva_rv                       double,
    packs_rv                     double,
    top_up_ex_rv                 double,
    top_up_co_rv                 double,
    gb_camp_rv                   double,
    others_rv                    double,
    tot_rv                       double,
    top_up_rv                    double,
    itx_rv                       double,
    exp_itx_rv                   double,
    total_invoice_rv             double,
    subscription_id              string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_invoice (
    country_id                   int,
    month_id                     string,
    customer_id                  string,
    msisdn_id                    string,
    activation_dt                string,
    billing_cycle_id             string,
    quota_data_rv                double,
    quota_voice_rv               double,
    quota_mess_rv                double,
    quota_agg_rv                 double,
    traffic_data_rv              double,
    traffic_voice_rv             double,
    traffic_mess_rv              double,
    traffic_agg_rv               double,
    roaming_rv                   double,
    sva_rv                       double,
    packs_rv                     double,
    top_up_ex_rv                 double,
    top_up_co_rv                 double,
    gb_camp_rv                   double,
    others_rv                    double,
    tot_rv                       double,
    top_up_rv                    double,
    itx_rv                       double,
    exp_itx_rv                   double,
    total_invoice_rv             double,
    subscription_id              string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id                   int,
    month                        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_invoice_ext (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_customer_for_invoice (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_billing_cycle_for_invoice_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_dim_m_billing_cycle_for_invoice (
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


-------------------------------------------------------------------------------
-- Reference received files as external partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_invoice_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_invoice_ext
ADD IF NOT EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_customer_for_invoice_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_customer_for_invoice_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/CUSTOMER/month=${nominalTime}';

ALTER TABLE gbic_dq_dim_m_billing_cycle_for_invoice_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id}
);

ALTER TABLE gbic_dq_dim_m_billing_cycle_for_invoice_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id}
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/DIM_M_BILLING_CYCLE';

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
INSERT OVERWRITE TABLE gbic_dq_invoice
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    customer_id,
    msisdn_id,
    activation_dt,
    billing_cycle_id,
    quota_data_rv,
    quota_voice_rv,
    quota_mess_rv,
    quota_agg_rv,
    traffic_data_rv,
    traffic_voice_rv,
    traffic_mess_rv,
    traffic_agg_rv,
    roaming_rv,
    sva_rv,
    packs_rv,
    top_up_ex_rv,
    top_up_co_rv,
    gb_camp_rv,
    others_rv,
    tot_rv,
    top_up_rv,
    itx_rv,
    exp_itx_rv,
    total_invoice_rv,
    subscription_id
FROM  gbic_dq_invoice_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_customer_for_invoice
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
FROM gbic_dq_customer_for_invoice_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_dim_m_billing_cycle_for_invoice
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
FROM gbic_dq_dim_m_billing_cycle_for_invoice_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND upper(country_id) != 'COUNTRY_ID';
