!echo -e -- Ensure existence of database gbic_global;
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

!echo -e -- Use gbic_global;
USE {{ project.prefix }}gbic_global;


SET hive.execution.engine=mr;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;


!echo -e -- Ensure existence of external table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_traffic_data_ext (
    country_id       int,
    month_id         string,
    billing_cycle_dt string,
    billing_cycle_id string,
    subscription_id  string,
    msisdn_id        string,
    day_cd           int,
    time_range_cd    int,
    imei_num         bigint,
    total_qt         double,
    mb_2g_qt         double,
    mb_3g_qt         double,
    mb_4g_qt         double,
    mb_roamin        double
) COMMENT 'External table over TRAFFIC_DATA raw files'
PARTITIONED BY (
    gbic_op_id       string,
    month            string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_TRAFFIC_DATA';


!echo -e -- Ensure existence of partition ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' ) on external TRAFFIC_DATA table;
ALTER TABLE gbic_global_traffic_data_ext
  ADD IF NOT EXISTS PARTITION ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' )
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_TRAFFIC_DATA/month=${hivevar:month}';

!echo -e -- Ensure existence of external aggregated table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_traffic_data_ext_group (
    country_id       int,
    month_id         string,
    billing_cycle_dt string,
    billing_cycle_id string,
    subscription_id  string,
    msisdn_id        string,
    day_cd           int,
    time_range_cd    int,
    imei_num         bigint,
    total_qt         double,
    mb_2g_qt         double,
    mb_3g_qt         double,
    mb_4g_qt         double,
    mb_roamin        double
) COMMENT 'External table over TRAFFIC_DATA grouping raw files'
PARTITIONED BY (
    month            string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/TRAFFIC_DATA';

!echo -e -- Ensure existence of partition ( month='${hivevar:month}' ) on external TRAFFIC_DATA aggregated table;
INSERT OVERWRITE TABLE gbic_global_traffic_data_ext_group PARTITION ( month='${hivevar:month}') 
SELECT country_id,
       month_id,
       billing_cycle_dt,
       billing_cycle_id,
       subscription_id,
       msisdn_id,
       day_cd,
       time_range_cd,
       imei_num,
       SUM(total_qt),
       SUM(mb_2g_qt),
       SUM(mb_3g_qt),
       SUM(mb_4g_qt),
       SUM(mb_roamin) 
FROM gbic_global_traffic_data_ext
WHERE gbic_op_id='${hivevar:op}'
AND month='${hivevar:month}'
GROUP BY country_id,
         month_id,
         billing_cycle_dt,
         billing_cycle_id,
         subscription_id,
         msisdn_id,
         day_cd,
         time_range_cd,
         imei_num;

!echo -e -- Drop external traffic_data table;
DROP TABLE IF EXISTS gbic_global_traffic_data_ext;

!echo -e -- Drop external traffic_data table;
DROP TABLE IF EXISTS gbic_global_traffic_data_ext_group;
