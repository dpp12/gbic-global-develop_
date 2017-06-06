-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=MULTISIM;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_multisim (
    gbic_op_name string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    msisdn_main  string COMMENT 'Main Line. Unique identifier of the line encrypted as a String',
    msisdn_add   string COMMENT 'Additional line. Unique identifier of the line encrypted as a String'
) COMMENT 'Relation between main line and additional' 
PARTITIONED BY (
    gbic_op_id   int    COMMENT 'GBIC GLOBAL OPERATOR ID',
    month        string COMMENT 'Date of monthly files')
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
