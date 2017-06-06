-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=CUSTOMER;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_customer (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    ob_id                         string COMMENT 'Operator code: M (MOVIL) F (FIJO) C (CONVERGENTE)',
    party_type_cd                 int    COMMENT 'Type of customer identification code',
    party_identification_type_cd  string COMMENT 'Document Type Client',
    party_identification_num      string COMMENT 'Document number customer, encrypted as a String',
    customer_id                   string COMMENT 'Unique customer code',
    activation_dt                 string COMMENT 'Discharge date line',
    seg_local_cd                  string COMMENT 'Local organizational segment identifier code',
    seg_local_name                string COMMENT 'Local organizational segment description',
    seg_global_id                 int    COMMENT 'Global organizational segment identifier',
    seg_global_name               string COMMENT 'Global organizational segment description',
    birth_dt                      string COMMENT 'Date of Birth client',
    age_id                        int    COMMENT 'Age segment',
    gender_type_cd                string COMMENT 'Gender customer',
    org_name                      string COMMENT 'Company name',
    cust_education                string COMMENT 'Education customer',
    cust_prod_qt                  string COMMENT 'Number of products that the customer has contracted',
    cust_life_cycle               string COMMENT 'Customer life cycle',
    socioeconomic_level           string COMMENT 'Socioeconomic level'
) COMMENT 'All customers by month'
PARTITIONED BY (
    gbic_op_id                    int    COMMENT 'GBIC GLOBAL OPERATOR ID',
    month                         string COMMENT 'Date of monthly files')
STORED AS ORC;

-------------------------------------------------------------------------------
-- STAGING AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global_staging;

USE {{ project.prefix }}gbic_global_staging;

CREATE TABLE IF NOT EXISTS gbic_global_{{ item }}
LIKE {{ project.prefix }}gbic_global.gbic_global_{{ item }};

ALTER TABLE gbic_global_{{ item }}
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);
