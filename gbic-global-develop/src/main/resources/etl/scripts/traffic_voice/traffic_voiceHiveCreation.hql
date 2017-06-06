-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=TRAFFIC_VOICE;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_traffic_voice (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    billing_cycle_id              string COMMENT 'Billing cycle identifier',
    billing_cycle_des             string COMMENT 'Billing cycle description',
    billing_cycle_start_dt        string COMMENT 'Billing cycle start date',
    billing_cycle_end_dt          string COMMENT 'Billing cycle end date',
    billing_due_dt                string COMMENT 'Billing cycle due date',
    billing_rv_computes           int    COMMENT 'When revenue computes: 0 Same month, 1 Next month, 2 In two months, -1 Previous Month',
    subscription_id               string COMMENT 'Subscription identifier, encrypted as a String',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    day_cd                        int    COMMENT 'Type of day the call was made',
    time_range_cd                 int    COMMENT 'Time range call started. These rage are able to change in the future.',
    imei_num                      bigint COMMENT 'International Mobile System Equipment Identity)',
    call_offnet_fixed_out         int    COMMENT 'Total outgoing calls to Fix numbers Off Net',
    call_onnet_fixed_out          int    COMMENT 'Total outgoing calls to Fix numbers On Net',
    call_offnet_mobile_out        int    COMMENT 'Total National Outgoing Calls Off Net',
    call_onnet_mobile_out         int    COMMENT 'Total National Outgoing Calls On Net',
    call_international_out        int    COMMENT 'Total International outgoing calls',
    call_onnet_out_free           int    COMMENT 'Total outgoing calls Free',
    call_onnet_rcm_out            int    COMMENT 'Total outgoing calls to Movistar Corporation Net (RCM)',
    call_roaming_out              int    COMMENT 'Total outgoing calls Roaming',
    call_out_special_numbers      int    COMMENT 'Total outgoing calls to Short Numbers',
    call_fixed_in                 int    COMMENT 'Total ingoing calls from Fix num.',
    call_offnet_mobile_in         int    COMMENT 'Total National Incoming Calls Off Net',
    call_onnet_mobile_in          int    COMMENT 'Total National Incoming Calls On Net',
    call_roaming_in               int    COMMENT 'Total ingoing calls Roaming',
    call_international_in         int    COMMENT 'Total International ingoing calls',
    min_offnet_fixed_out          double COMMENT 'Total outgoing minutes Off Net to Fix number',
    min_onnet_fixed_out           double COMMENT 'Total outgoing minutes On Net to Fix number',
    min_offnet_mobile_out         double COMMENT 'Total outgoing minutes to mobiles Off Net',
    min_onnet_mobile_out          double COMMENT 'Total outgoing minutes to mobiles On Net',
    min_international_out         double COMMENT 'Total outgoing international minutes',
    min_onnet_free_out            double COMMENT 'Total outgoing minutes to Free numbers.',
    min_onnet_rcm_out             double COMMENT 'Total outgoing minutes On Net to Movistar Corporation Net (RCM)',
    min_roaming_out               double COMMENT 'Total outgoing minutes Roaming',
    min_out_special_numbers       double COMMENT 'Total outgoing minutes to short numbers',
    min_fixed_in                  double COMMENT 'Total ingoing minutes from Fix numbers',
    min_offnet_mobile_in          double COMMENT 'Total Minutes Starters Off Net',
    min_onnet_mobile_in           double COMMENT 'Total Minutes Starters On Net',
    min_roaming_in                double COMMENT 'Total ingoing minutes Roaming',
    min_international_in          double COMMENT 'Total ingoing minutes',
    min_fixed_out_bundled         double COMMENT 'Total outgoing minutes a Fijos incuded in the Bundle',
    min_mobile_out_bundled        double COMMENT 'Total outgoing minutes to Mobile incuded in the Bundle',
    min_fixed_out_not_bundled     double COMMENT 'Total outgoing minutes to Fix Not incuded in the Bundle',
    min_mobile_out_not_bundled    double COMMENT 'Total outgoing minutes to Mobile Not incuded in the Bundle',
    min_fixed_out_exceed          double COMMENT 'To Total outgoing minutes to Mobile Exceeded',
    min_mobile_out_exceed         double COMMENT 'Total outgoing minutes to Fix Exceeded',
    roaming_rv                    double COMMENT 'Roaming Revenues',
    out_other_rv                  double COMMENT 'Outgoing Revenues',
    out_national_onnet_rv         double COMMENT 'National Outgoing Revenues On Net',
    out_national_offnet_rv        double COMMENT 'National Outgoing Revenues Off Net',
    out_national_fixed_rv         double COMMENT 'National Outgoing Revenues to Fix',
    out_international_rv          double COMMENT 'International Outgoing Revenues',
    call_2g_out                   int    COMMENT 'Outcogoing calls in  2G net',
    call_3g_out                   int    COMMENT 'Outcogoing calls in  3G net',
    call_4g_out                   int    COMMENT 'Outcogoing calls in  4G net',
    call_2g_in                    int    COMMENT 'Ingoing calls in  2G net',
    call_3g_in                    int    COMMENT 'Ingoing calls in  3G net',
    call_4g_in                    int    COMMENT 'Ingoing calls in  4G net',
    min_2g_out                    double COMMENT 'Outcogoing minutes in  2G net',
    min_3g_out                    double COMMENT 'Outcogoing minutes in  3G net',
    min_4g_out                    double COMMENT 'Outcogoing minutes in  4G net',
    min_2g_in                     double COMMENT 'Ingoing minutes in  2G net',
    min_3g_in                     double COMMENT 'Ingoing minutes in  3G net',
    min_4g_in                     double COMMENT 'Ingoing minutes in  4G net '
) COMMENT 'All IMEIS by month and MSISDN'
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
