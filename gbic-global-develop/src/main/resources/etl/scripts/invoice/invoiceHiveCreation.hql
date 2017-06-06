-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=invoice;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=invoice;

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_invoice(
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    customer_id                   string COMMENT 'Customer ifentifier',
    msisdn_id                     string COMMENT 'Unique identifier of the line encrypted as a String',
    activation_dt                 string COMMENT 'Discharge date line',
    billing_cycle_id              string COMMENT 'Billing period',
    billing_cycle_des             string COMMENT 'Billing period description',
    quota_data_rv                 double COMMENT 'Data revenues',
    quota_voice_rv                double COMMENT 'Voice revenues',
    quota_mess_rv                 double COMMENT 'Messaging revenues',
    quota_agg_rv                  double COMMENT 'Aggregate revenues',
    traffic_data_rv               double COMMENT 'Traffic Data revenues',
    traffic_voice_rv              double COMMENT 'Traffic Voice revenues',
    traffic_mess_rv               double COMMENT 'Traffic Messaging revenues',
    traffic_agg_rv                double COMMENT 'Traffic Aggregate revenues',
    roaming_rv                    double COMMENT 'Roaming revenues',
    sva_rv                        double COMMENT 'Revenue from value-added services',
    packs_rv                      double COMMENT 'Income from hired packages',
    top_up_ex_rv                  double COMMENT 'Income balances expired',
    top_up_co_rv                  double COMMENT 'Income consumption refills',
    gb_camp_rv                    double COMMENT 'Returns and campaigns',
    others_rv                     double COMMENT 'Other income not fall into any classification',
    tot_rv                        double COMMENT 'total revenue',
    top_up_rv                     double COMMENT 'Total amount recharged',
    itx_rv                        double COMMENT 'Interconnection revenues (voice + sms)',
    exp_itx_rv                    double COMMENT 'Interconnection expenses (voice + sms)',
    total_invoice_rv              double COMMENT 'Sum of total revenue billed to the line',
    subscription_id               string COMMENT 'Subscription identifier'
) COMMENT 'Invoice for all lines (Postpaid, Hibrid and Prepaid) by month and MSISDN'
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
