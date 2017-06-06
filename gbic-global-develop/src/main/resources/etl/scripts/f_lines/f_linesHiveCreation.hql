-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=F_LINES;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global;

USE {{ project.prefix }}gbic_global;

CREATE TABLE IF NOT EXISTS gbic_global_f_lines (
    gbic_op_name                  string COMMENT 'GBIC GLOBAL OPERATOR NAME',
    currency                      string COMMENT 'Type of currency',
    subscription_id               string COMMENT 'Subscription identifier',
    administrator_id              string COMMENT 'Unique identifier of the circuit code for the Broadband',
    customer_id                   string COMMENT 'Customer identifier',
    fix_customer_id               string COMMENT 'Customer Unique Code for fix lines',
    postal_cd                     string COMMENT 'Postal Unique Code for fix lines',
    party_type_cd                 bigint COMMENT 'Client type id',
    seg_local_cd                  string COMMENT 'Local organizational segment identifier code',
    seg_local_name                string COMMENT 'Local organizational segment name',
    seg_global_id                 int    COMMENT 'Global organizational segment identifier',
    seg_global_name               string COMMENT 'Global organizational segment name',
    voice_ind                     int    COMMENT 'Indicates if the line has an associated voice service',
    voice_activation_dt           string COMMENT 'Date of activation of the voice service',
    voice_type_cd                 string COMMENT 'Type of voice line',
    voice_tariff_plan_id          string COMMENT 'ID of the voice tariff plan',
    voice_tariff_plan_des         string COMMENT 'Description of the voice tariff plan',
    voice_month_rv                double COMMENT 'Total voice revenues',
    bband_ind                     int    COMMENT 'Indicates if the line has an associated broadband service',
    bband_activation_dt           string COMMENT 'Date of activation of the bband service',
    bband_type_cd                 string COMMENT 'Type of bband line',
    bband_tariff_plan_id          string COMMENT 'ID of the bband tariff plan',
    bband_tariff_plan_des         string COMMENT 'Description of the bbrand tariff plan',
    speed_band_qt                 int    COMMENT 'Bandwidth',
    bband_month_rv                double COMMENT 'Total bband revenues',
    tv_ind                        int    COMMENT 'Indicates if the line has a TV service associated',
    tv_sales_dt                   string COMMENT 'Hire date of TV service',
    tv_activation_dt              string COMMENT 'Setup date of TV service (not actual activation of service)',
    tv_use_dt                     string COMMENT 'Activation date service (actual activation of service on first usage)',
    tv_promo_id                   int    COMMENT 'Indicates if the client enjoys a promotion',
    tv_end_promo_dt               string COMMENT 'End date of the promotion',
    tv_type_cd                    string COMMENT 'Type of TV line',
    tv_tariff_plan_id             string COMMENT 'Identifying code of TV package, product or tariff',
    tv_tariff_plan_des            string COMMENT 'Descriptive name of TV package, product or tariff',
    tv_points_qt                  int    COMMENT 'Number of connections on the residence',
    tv_recurring_rv               double COMMENT 'Revenues for TV subscription',
    tv_non_recurring_rv           double COMMENT 'Revenues for renting movies, concerts...',
    tv_month_rv                   double COMMENT 'Total revenues for the TV service',
    workstation_ind               int    COMMENT 'Indicates if the line has an associated workstation',
    workstation_type_cd           string COMMENT 'Type of workstation',
    app_ind                       int    COMMENT 'Indicates if the line has an associated Aplicateca service',
    total_month_rv                double COMMENT 'Total revenues for the whole line',
    data_consumed_qt              double COMMENT 'Data consumed by line',
    calls_voice_qt                double COMMENT 'Number of calls made (outgoing ) by line',
    minutes_voice_qt              double COMMENT 'Minutes consumed (outgoing ) by line'
) COMMENT 'Land lines by month and customer'
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
