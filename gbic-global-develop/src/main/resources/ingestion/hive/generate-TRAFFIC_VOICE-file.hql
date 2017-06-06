!echo -e -- Ensure existence of database gbic_global;
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

!echo -e -- Use gbic_global;
USE {{ project.prefix }}gbic_global;


SET hive.execution.engine=mr;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;


!echo -e -- Ensure existence of external table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_traffic_voice_ext (
    country_id                 int,
    month_id                   string,
    billing_cycle_dt           string,
    billing_cycle_id           string,
    subscription_id            string,
    msisdn_id                  string,
    day_cd                     int,
    time_range_cd              int,
    imei_num                   bigint,
    call_offnet_fixed_out      int,
    call_onnet_fixed_out       int,
    call_offnet_mobile_out     int,
    call_onnet_mobile_out      int,
    call_international_out     int,
    call_onnet_out_free        int,
    call_onnet_rcm_out         int,
    call_roaming_out           int,
    call_out_special_numbers   int,
    call_fixed_in              int,
    call_offnet_mobile_in      int,
    call_onnet_mobile_in       int,
    call_roaming_in            int,
    call_international_in      int,
    min_offnet_fixed_out       double,
    min_onnet_fixed_out        double,
    min_offnet_mobile_out      double,
    min_onnet_mobile_out       double,
    min_international_out      double,
    min_onnet_free_out         double,
    min_onnet_rcm_out          double,
    min_roaming_out            double,
    min_out_special_numbers    double,
    min_fixed_in               double,
    min_offnet_mobile_in       double,
    min_onnet_mobile_in        double,
    min_roaming_in             double,
    min_international_in       double,
    min_fixed_out_bundled      double,
    min_mobile_out_bundled     double,
    min_fixed_out_not_bundled  double,
    min_mobile_out_not_bundled double,
    min_fixed_out_exceed       double,
    min_mobile_out_exceed      double,
    roaming_rv                 double,
    out_other_rv               double,
    out_national_onnet_rv      double,
    out_national_offnet_rv     double,
    out_national_fixed_rv      double,
    out_international_tv       double,
    call_2g_out                int,
    call_3g_out                int,
    call_4g_out                int,
    call_2g_in                 int,
    call_3g_in                 int,
    call_4g_in                 int,
    min_2g_out                 double,
    min_3g_out                 double,
    min_4g_out                 double,
    min_2g_in                  double,
    min_3g_in                  double,
    min_4g_in                  double
) COMMENT 'External table over TRAFFIC_VOICE raw files'
PARTITIONED BY (
    gbic_op_id                 string,
    month                      string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_TRAFFIC_VOICE';


!echo -e -- Ensure existence of partition ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' ) on external TRAFFIC_VOICE table;
ALTER TABLE gbic_global_traffic_voice_ext
  ADD IF NOT EXISTS PARTITION ( gbic_op_id='${hivevar:op}', month='${hivevar:month}' )
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/raw_TRAFFIC_VOICE/month=${hivevar:month}';

!echo -e -- Ensure existence of external aggregated table;
CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_traffic_voice_ext_group (
    country_id                 int,
    month_id                   string,
    billing_cycle_dt           string,
    billing_cycle_id           string,
    subscription_id            string,
    msisdn_id                  string,
    day_cd                     int,
    time_range_cd              int,
    imei_num                   bigint,
    call_offnet_fixed_out      int,
    call_onnet_fixed_out       int,
    call_offnet_mobile_out     int,
    call_onnet_mobile_out      int,
    call_international_out     int,
    call_onnet_out_free        int,
    call_onnet_rcm_out         int,
    call_roaming_out           int,
    call_out_special_numbers   int,
    call_fixed_in              int,
    call_offnet_mobile_in      int,
    call_onnet_mobile_in       int,
    call_roaming_in            int,
    call_international_in      int,
    min_offnet_fixed_out       double,
    min_onnet_fixed_out        double,
    min_offnet_mobile_out      double,
    min_onnet_mobile_out       double,
    min_international_out      double,
    min_onnet_free_out         double,
    min_onnet_rcm_out          double,
    min_roaming_out            double,
    min_out_special_numbers    double,
    min_fixed_in               double,
    min_offnet_mobile_in       double,
    min_onnet_mobile_in        double,
    min_roaming_in             double,
    min_international_in       double,
    min_fixed_out_bundled      double,
    min_mobile_out_bundled     double,
    min_fixed_out_not_bundled  double,
    min_mobile_out_not_bundled double,
    min_fixed_out_exceed       double,
    min_mobile_out_exceed      double,
    roaming_rv                 double,
    out_other_rv               double,
    out_national_onnet_rv      double,
    out_national_offnet_rv     double,
    out_national_fixed_rv      double,
    out_international_tv       double,
    call_2g_out                int,
    call_3g_out                int,
    call_4g_out                int,
    call_2g_in                 int,
    call_3g_in                 int,
    call_4g_in                 int,
    min_2g_out                 double,
    min_3g_out                 double,
    min_4g_out                 double,
    min_2g_in                  double,
    min_3g_in                  double,
    min_4g_in                  double
) COMMENT 'External table over TRAFFIC_VOICE grouping raw files'
PARTITIONED BY (
    month                      string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '{{ hdfs.inbox }}/${hivevar:op3m}/MSv${hivevar:version}/TRAFFIC_VOICE';

!echo -e -- Ensure existence of partition ( month='${hivevar:month}' ) on external TRAFFIC_VOICE aggregated table;
INSERT OVERWRITE TABLE gbic_global_traffic_voice_ext_group PARTITION ( month='${hivevar:month}') 
SELECT country_id,
       month_id,
       billing_cycle_dt,
       billing_cycle_id,
       subscription_id,
       msisdn_id,
       day_cd,
       time_range_cd,
       imei_num,
       SUM(call_offnet_fixed_out),
       SUM(call_onnet_fixed_out),
       SUM(call_offnet_mobile_out),
       SUM(call_onnet_mobile_out),
       SUM(call_international_out),
       SUM(call_onnet_out_free),
       SUM(call_onnet_rcm_out),
       SUM(call_roaming_out),
       SUM(call_out_special_numbers),
       SUM(call_fixed_in),
       SUM(call_offnet_mobile_in),
       SUM(call_onnet_mobile_in),
       SUM(call_roaming_in),
       SUM(call_international_in),
       SUM(min_offnet_fixed_out),
       SUM(min_onnet_fixed_out),
       SUM(min_offnet_mobile_out),
       SUM(min_onnet_mobile_out),
       SUM(min_international_out),
       SUM(min_onnet_free_out),
       SUM(min_onnet_rcm_out),
       SUM(min_roaming_out),
       SUM(min_out_special_numbers),
       SUM(min_fixed_in),
       SUM(min_offnet_mobile_in),
       SUM(min_onnet_mobile_in),
       SUM(min_roaming_in),
       SUM(min_international_in),
       SUM(min_fixed_out_bundled),
       SUM(min_mobile_out_bundled),
       SUM(min_fixed_out_not_bundled),
       SUM(min_mobile_out_not_bundled),
       SUM(min_fixed_out_exceed),
       SUM(min_mobile_out_exceed),
       SUM(roaming_rv),
       SUM(out_other_rv),
       SUM(out_national_onnet_rv),
       SUM(out_national_offnet_rv),
       SUM(out_national_fixed_rv),
       SUM(out_international_tv),
       SUM(call_2g_out),
       SUM(call_3g_out),
       SUM(call_4g_out),
       SUM(call_2g_in),
       SUM(call_3g_in),
       SUM(call_4g_in),
       SUM(min_2g_out),
       SUM(min_3g_out),
       SUM(min_4g_out),
       SUM(min_2g_in),
       SUM(min_3g_in),
       SUM(min_4g_in)
FROM gbic_global_traffic_voice_ext
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


!echo -e -- Drop external traffic_voice table;
DROP TABLE IF EXISTS gbic_global_traffic_voice_ext;

!echo -e -- Drop external traffic_voice table;
DROP TABLE IF EXISTS gbic_global_traffic_voice_ext_group;
