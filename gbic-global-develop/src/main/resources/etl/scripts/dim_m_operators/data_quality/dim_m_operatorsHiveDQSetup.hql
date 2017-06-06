-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=DIM_M_OPERATORS;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_m_operators_ext (
    country_id  int,
    month_id    string,
    port_op_cd  string,
    port_op_des string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id  int,
    month       string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_dim_m_operators (
    country_id  int,
    month_id    string,
    port_op_cd  string,
    port_op_des string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id  int,
    month       string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");

-------------------------------------------------------------------------------
-- Reference received files as external partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_dim_m_operators_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_operators_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

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
INSERT OVERWRITE TABLE gbic_dq_dim_m_operators
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    port_op_cd,
    port_op_des
FROM gbic_dq_dim_m_operators_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';
