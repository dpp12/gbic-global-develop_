!echo -e -- Ensure existence of database gbic_global;
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

!echo -e -- Use gbic_global;
USE {{ project.prefix }}gbic_global;


SET hive.execution.engine=mr;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;


!echo -e -- Ensure existence of external table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_daily_traffic_ext (
    country_id      int,
    month_id        string,
    call_dt         string,
    msisdn_id       string,
    imei_num        bigint,
    day_type_cd     string,
    bank_holiday_cd int,
    roaming_cd      int,
    calls_total_qt  int,
    minutes_tot_qt  double,
    sms_total_qt    int,
    mb_total_qt     double,
    subscription_id string
) COMMENT 'External table over DAILY_TRAFFIC raw files'
PARTITIONED BY (
    gbic_op_id      string,
    month           string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_DAILY_TRAFFIC';


!echo -e -- Ensure existence of partition ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' ) on external DAILY_TRAFFIC table;
ALTER TABLE gbic_global_daily_traffic_ext
  ADD IF NOT EXISTS PARTITION ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' )
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_DAILY_TRAFFIC/month=${hivevar:month}';

!echo -e -- Ensure existence of external aggregated table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_daily_traffic_ext_group (
    country_id      int,
    month_id        string,
    call_dt         string,
    msisdn_id       string,
    imei_num        bigint,
    day_type_cd     string,
    bank_holiday_cd int,
    roaming_cd      int,
    calls_total_qt  int,
    minutes_tot_qt  double,
    sms_total_qt    int,
    mb_total_qt     double,
    subscription_id string
) COMMENT 'External table over DAILY_TRAFFIC grouping raw files'
PARTITIONED BY (
    month           string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/DAILY_TRAFFIC';

!echo -e -- Ensure existence of partition ( month='${hivevar:month}' ) on external DAILY_TRAFFIC aggregated table;
INSERT OVERWRITE TABLE gbic_global_daily_traffic_ext_group PARTITION ( month='${hivevar:month}') 
SELECT country_id,
       month_id,
       call_dt,
       msisdn_id,
       imei_num,
       day_type_cd,
       bank_holiday_cd,
       roaming_cd,
       SUM(calls_total_qt),
       SUM(minutes_tot_qt),
       SUM(sms_total_qt),
       SUM(mb_total_qt),
       subscription_id 
FROM gbic_global_daily_traffic_ext
WHERE gbic_op_id='${hivevar:op}'
AND month='${hivevar:month}'
GROUP BY country_id,
         month_id,
         call_dt,
         msisdn_id,
         imei_num,
         day_type_cd,
         bank_holiday_cd,
         roaming_cd,
         subscription_id;

!echo -e -- Drop external DAILY_TRAFFIC table;
DROP TABLE IF EXISTS gbic_global_daily_traffic_ext;

!echo -e -- Drop external DAILY_TRAFFIC table;
DROP TABLE IF EXISTS gbic_global_daily_traffic_ext_group;
