-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--                                                                           --
--  DATA QUALITY AREA                                                        --
--                                                                           --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--                                                                           --
--      DDL for data quality tables                                          --
--          * Service checks                                                 --
--          * File test                                                      --
--          * Screen results                                                 --
--          * Test result                                                    --
--                                                                           --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global_dq;

USE {{ project.prefix }}gbic_global_dq;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_service_checks (
    processing_date string,
    content_date    string,
    project_name    string,
    file_name       string,
    countryIso3     string,
    screenNumber    int,
    screenDesc      string,
    field           string,
    fieldContent    string,
    fieldValue      double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id      int,
    file            string,
    day             string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE EXTERNAL TABLE IF NOT EXISTS gbic_dq_file_test (
    id_filerevision     int,
    id_fileentity       int,
    id_test             int,
    test_type           string,
    test_number_file    string,
    file_process_date   string,
    test_desc           string,
    test_field          string,
    test_field_content  string,
    test_expected_value string,
    error_threshold     string,
    warn_threshold      string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id          int,
    file                string,
    day                 string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS gbic_dq_screen_results (
    field        string,
    fieldContent string,
    fieldValue   double
) COMMENT ''
PARTITIONED BY (
    gbic_op_id   int,
    file         string,
    day          string,
    screenId     string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.screenres }}';

CREATE TABLE IF NOT EXISTS gbic_dq_test_result (
    id_filerevision      int,
    id_test              int,
    test_type            string,
    screen_number        int,
    test_number_file     int,
    test_field           string,
    test_field_content   string,
    test_expected_value  double,
    test_resulting_value double,
    warn_threshold       float,
    error_threshold      float,
    test_state           string
) COMMENT ''
PARTITIONED BY (
    gbic_op_id           int,
    file                 string,
    day                  string,
    test_id              int
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.testres }}';
