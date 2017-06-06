-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=M_LINE_SERVICES;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_m_line_services (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    subscription_id               string COMMENT 'Subscription identifier, encrypted as a String',
    activation_dt                 string COMMENT 'Activation date for the line',
    id_service                    string COMMENT 'Unique service Identifier',
    des_service                   string COMMENT 'Unique service description',
    operation_cd                  string COMMENT 'Service Operation Code',
    group_sva                     string COMMENT 'Group sva',
    group_sva_des                 string COMMENT 'Group sva description',
    service_activ_dt              string COMMENT 'Activation Date',
    recurrent_ind                 int    COMMENT 'Service is recurrent or not'
) COMMENT 'All service new adds and churns by month and MSISDN' 
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
