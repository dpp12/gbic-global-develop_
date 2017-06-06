!echo -e -- Ensure existence of database gbic_global;
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

!echo -e -- Use gbic_global;
USE {{ project.prefix }}gbic_global;


SET hive.execution.engine=mr;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;


!echo -e -- Ensure existence of external table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_traffic_sms_ext (
    country_id               int,
    month_id                 string,
    billing_cycle_dt         string,
    billing_cycle_id         string,
    subscription_id          string,
    msisdn_id                string,
    day_cd                   int,
    time_range_cd            int,
    imei_num                 bigint,
    sms_offnet_out_qt        int,
    sms_onnet_out_qt         int,
    sms_international_out_qt int,
    sms_roaming_out_qt       int,
    sms_offnet_in_qt         int,
    sms_onnet_in_qt          int,
    sms_international_in_qt  int,
    sms_roaming_in_qt        int,
    sms_out_bundled_rv       double,
    sms_out_not_bundled_rv   double,
    sms_roaming_out_rv       double
) COMMENT 'External table over TRAFFIC_SMS raw files'
PARTITIONED BY (
    gbic_op_id                string,
    month                     string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_TRAFFIC_SMS';


!echo -e -- Ensure existence of partition ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' ) on external TRAFFIC_SMS table;
ALTER TABLE gbic_global_traffic_sms_ext
  ADD IF NOT EXISTS PARTITION ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' )
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_TRAFFIC_SMS/month=${hivevar:month}';

!echo -e -- Ensure existence of external aggregated table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_traffic_sms_ext_group (
    country_id               int,
    month_id                 string,
    billing_cycle_dt         string,
    billing_cycle_id         string,
    subscription_id          string,
    msisdn_id                string,
    day_cd                   int,
    time_range_cd            int,
    imei_num                 bigint,
    sms_offnet_out_qt        int,
    sms_onnet_out_qt         int,
    sms_international_out_qt int,
    sms_roaming_out_qt       int,
    sms_offnet_in_qt         int,
    sms_onnet_in_qt          int,
    sms_international_in_qt  int,
    sms_roaming_in_qt        int,
    sms_out_bundled_rv       double,
    sms_out_not_bundled_rv   double,
    sms_roaming_out_rv       double
) COMMENT 'External table over TRAFFIC_SMS grouping raw files'
PARTITIONED BY (
    month                    string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/TRAFFIC_SMS';

!echo -e -- Ensure existence of partition ( month='${hivevar:month}' ) on external TRAFFIC_SMS aggregated table;
INSERT OVERWRITE TABLE gbic_global_traffic_sms_ext_group PARTITION ( month='${hivevar:month}') 
SELECT country_id,
       month_id,
       billing_cycle_dt,
       billing_cycle_id,
       subscription_id,
       msisdn_id,
       day_cd,
       time_range_cd,
       imei_num,
       SUM(sms_offnet_out_qt),
       SUM(sms_onnet_out_qt),
       SUM(sms_international_out_qt),
       SUM(sms_roaming_out_qt),
       SUM(sms_offnet_in_qt),
       SUM(sms_onnet_in_qt),
       SUM(sms_international_in_qt),
       SUM(sms_roaming_in_qt),
       SUM(sms_out_bundled_rv),
       SUM(sms_out_not_bundled_rv),
       SUM(sms_roaming_out_rv)
FROM gbic_global_traffic_sms_ext
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


!echo -e -- Drop external traffic_sms table;
DROP TABLE IF EXISTS gbic_global_traffic_sms_ext;

!echo -e -- Drop external traffic_sms table;
DROP TABLE IF EXISTS gbic_global_traffic_sms_ext_group;
