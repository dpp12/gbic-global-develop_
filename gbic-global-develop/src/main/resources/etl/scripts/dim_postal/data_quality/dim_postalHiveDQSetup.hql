-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=DIM_POSTAL;
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_dim_postal_ext (
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

CREATE TABLE IF NOT EXISTS gbic_dq_dim_postal (
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

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_location_for_dimpostal_ext (
    loc_month    string,
    concept_id   int,
    concept_name string,
    gbic_op_id   int,
    local_cd     string,
    local_name   string,
    global_id    int,
    global_name  string
) COMMENT 'External table over dim_postal raw files'
PARTITIONED BY (
    month            string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_location_for_dimpostal (
    loc_month    string,
    concept_id   int,
    concept_name string,
    local_cd     string,
    local_name   string,
    global_id    int,
    global_name  string
) COMMENT 'ORC table over dim_postal raw files'
PARTITIONED BY (
    gbic_op_id   int,
    month        string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  STORED AS ORC
  TBLPROPERTIES ("orc.compress"="ZLIB");


-------------------------------------------------------------------------------
-- Reference received files as external partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_dim_postal_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_postal_ext
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) LOCATION '{{ hdfs.inbox }}/${ob}/${version}/${upperFileName}/month=${nominalTime}';

ALTER TABLE gbic_dq_location_for_dimpostal_ext
DROP IF EXISTS PARTITION (
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_location_for_dimpostal_ext
ADD PARTITION (
    month = '${nominalTime}'
) LOCATION '{{ cluster.service }}/homog/month=${nominalTime}/dim=2';

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
INSERT OVERWRITE TABLE gbic_dq_dim_postal
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    country_id,
    month_id,
    postal_id,
    location_level,
    location_name
FROM gbic_dq_dim_postal_ext
WHERE gbic_op_id = ${gbic_op_id}
  AND month = '${nominalTime}'
  AND upper(month_id) != 'MONTH_ID';

INSERT OVERWRITE TABLE gbic_dq_location_for_dimpostal
PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
) SELECT
    loc_month,
    concept_id,
    concept_name,
    local_cd,
    local_name,
    global_id,
    global_name
FROM gbic_dq_location_for_dimpostal_ext
WHERE month = '${nominalTime}'
  AND gbic_op_id = ${gbic_op_id};
