CREATE DATABASE IF NOT EXISTS {{ db.schema }};

USE {{ db.schema }};

-- ***************************
-- GLOBAL AUX TABLES
-- ***************************
DROP TABLE IF EXISTS gbic_global_operators;

CREATE TABLE gbic_global_operators(
   gbic_op_id        int,
   gbic_op_name      varchar(100),
   country_iso2      varchar(2),
   country_iso3      varchar(3),
   primary key (gbic_op_id)
);


-- ***************************
-- TABLES WITH KPIS
-- ***************************
DROP TABLE IF EXISTS gbic_global_kpis_mobile;

CREATE TABLE gbic_global_kpis_mobile(
    gbic_op_id              int         COMMENT 'Operator Global identifier',
    seg_global_id           int         COMMENT 'Organization Segment Identifier.See table GBIC_GLOBAL_STD_GLOBAL_CONCEPTS->concept_id=1',
    pre_post_id             varchar(1)  COMMENT 'P->Prepaid, C->Contract, X->Hybrid',
    gbic_customer_id        int         COMMENT 'Global Customer Identifier. See tables GBIC_GLOBAL_DIMS_CUSTOMERS',
    device_id               int         COMMENT 'Tac Identifier. See tables GBIC_GLOBAL_DIMS_TACS',
    tariff_plan_id          int         COMMENT 'Global Tariff Identifier. See table GBIC_GLOBAL_DIMS_M_TARIFFS',
    gbic_geo_zone_id        int         COMMENT 'Identifier of geo position',
    months_old              int         COMMENT 'Groups of months since activation', 
    prod_type_cd            varchar(1)  COMMENT 'Prod type indicator',
    bta_ind                 int         COMMENT 'Indicator about if lines are stable according of OB criteria',
    multisim_ind            int         COMMENT 'Indicator about multisim. 1 -> Multisim line but not the main. 0 -> Main lines being or not multisim',
    month                   date        COMMENT 'Month in format YYYY-MM-DD',
    n_lines_total           bigint      COMMENT '# total of lines',
    n_lines_exceed          bigint      COMMENT '# of lines exceeding their allowance',
    n_lines_data_tariff     bigint      COMMENT '# of lines which have a data tariff',
    n_lines_extra_data      bigint      COMMENT '# of lines buying extras',
    n_lines_exceed_extra    bigint      COMMENT '# of lines exceeding their allowance and buying extras',
    vl_data_bundled         double      COMMENT '# Mb of data bundled',
    vl_data_exceed          double      COMMENT '# Mb of data consumed over the bundled ones by users exceeding the allowance',
    vl_data_consumed        double      COMMENT '# Mb of data consumed',
    vl_voice_consumed       double      COMMENT '# Minutes of voice consumed',
    vl_sms_consumed         double      COMMENT '# Sms consumed',
    n_voice_calls           double      COMMENT '# Voice calls',
    extra_data_rv           double      COMMENT 'Revenues from buying extras',
    total_rv                double      COMMENT 'Total revenues',
    PRIMARY KEY (gbic_op_id,seg_global_id,pre_post_id,gbic_customer_id,device_id,tariff_plan_id,gbic_geo_zone_id,months_old,prod_type_cd,bta_ind,multisim_ind,month)
);


-- ***************************
-- TABLES WITH AGGREGATED DATA
-- ***************************
DROP TABLE IF EXISTS report_b2c_bta;

CREATE TABLE IF NOT EXISTS report_b2c_bta
(
    gbic_op_id              int           COMMENT 'Operator Global Name',
    month                   date          COMMENT 'Month',
    bta_ind                 int           COMMENT 'Beyond Allowance Indicator',
    loc_lev_7               varchar(512)  COMMENT 'Commercial distrit',
    prod_type_cd            varchar(1)    COMMENT 'Prod type indicator',
    gbic_tariff_id          int           COMMENT 'Global Tariff Identifier. See table GBIC_GLOBAL_DIMS_TARIFFS',
    is_data_tariff          varchar(16)    COMMENT 'Indicator of data plan',
    des_plan                varchar(256)  COMMENT 'Description of plan',
    des_manufact            varchar(32)  COMMENT 'Description of manufact',
    des_model               varchar(512)  COMMENT 'Device Model',
    os                      varchar(32)  COMMENT 'System operator',
    technology              varchar(10)   COMMENT 'Technology',
    market_category         varchar(64)   COMMENT 'Market category (smartphonetablet…)',
    months_old              int           COMMENT 'Groups of months since activation',
    n_lines_total           int           COMMENT '# total of lines include multisim',
    n_lines_princial        int           COMMENT '# total of lines principal',
    n_lines_exceed          int           COMMENT '# of lines exceeding their allowance',
    n_lines_data_tariff     int           COMMENT '# of lines which have a data tariff',
    n_lines_extra_data      int           COMMENT '# of lines buying extras',
    n_lines_exceed_extra    int           COMMENT '# of lines exceeding their allowance and buying extras',
    n_voice_calls           int           COMMENT '# Voice calls',
    vl_data_bundled         double        COMMENT '# Mb of data bundled',
    vl_data_exceed          double        COMMENT '# Mb of data consumed over the bundled ones by users exceedingthe allowance',
    vl_data_consumed        double        COMMENT '# Mb of data consumed',
    vl_voice_consumed       double        COMMENT '# Minutes of voice consumed',
    vl_sms_consumed         double        COMMENT '# Sms consumed',
    quota_agg_rv            double        COMMENT 'Revenues from quota not in data voice and mess',
    quota_data_rv           double        COMMENT 'Revenues from quota data',
    quota_voice_rv          double        COMMENT 'Revenues from quota voice',
    quota_mess_rv           double        COMMENT 'Revenues from quota mess',
    traffic_agg_rv          double        COMMENT 'Revenues from traffic not in data voice and mess',
    traffic_data_rv         double        COMMENT 'Revenues from traffic data',
    traffic_voice_rv        double        COMMENT 'Revenues from traffic voice',
    traffic_mess_rv         double        COMMENT 'Revenues from traffic mess',
    roaming_rv              double        COMMENT 'Revenues from roaming',
    sva_rv                  double        COMMENT 'Revenue from value-added services',
    packs_rv                double        COMMENT 'Income from hired packages',
    top_up_ex_rv            double        COMMENT 'Income balances expired',
    top_up_co_rv            double        COMMENT 'Income consumption refills',
    gb_camp_rv              double        COMMENT 'Returns and campaigns',
    others_rv               double        COMMENT 'Other income not fall into any classification',
    tot_rv                  double        COMMENT 'total revenue without top ups',
    top_up_rv               double        COMMENT 'Total amount recharged',
    itx_rv                  double        COMMENT 'Interconnection revenues (voice + sms)',
    exp_itx_rv              double        COMMENT 'Interconnection expenses (voice + sms)',
    extra_data_rv           double        COMMENT 'Revenues from extra data packages from m_lines',
    total_invoice_rv        double        COMMENT 'Sum of total revenue billed to the line'
);


-- **************************************
-- TABLES WITH DIMENSIONS
-- **************************************
DROP TABLE IF EXISTS gbic_global_dims_geo_zones;
DROP TABLE IF EXISTS gbic_global_dims_tacs;
DROP TABLE IF EXISTS gbic_global_dims_tacs_history;
DROP TABLE IF EXISTS gbic_global_dims_m_tariffs;
DROP TABLE IF EXISTS gbic_global_dims_m_tariffs_history;
DROP TABLE IF EXISTS gbic_global_dims_customers;
DROP TABLE IF EXISTS gbic_global_dims_customers_history; 

CREATE TABLE gbic_global_dims_tacs(
    device_id           int             COMMENT 'Tac id',
    des_manufact        varchar(128)    COMMENT 'Device manufacturer',
    des_model           varchar(128)    COMMENT 'Device model',
    os                  varchar(128)    COMMENT 'Device OS',
    version_os          varchar(128)    COMMENT 'Version of the OS',
    technology          varchar(10)     COMMENT '4G,3G,2G',
    activation_date     date            COMMENT 'Month when a device is added',
    deactivation_date   date            COMMENT 'Month when a device is deleted',
    PRIMARY KEY (device_id)
) COMMENT 'Table storing tacs snapshot by month. Includes SCD type 1 attributes of the tacs'
;

CREATE TABLE gbic_global_dims_tacs_history(
    device_id           int         COMMENT 'Tac id',
    date_ini            date        COMMENT 'Initial date of the state',
    date_end            date        COMMENT 'End date of the state',
    market_category     varchar(32) COMMENT 'Device market category',
    tef_category        varchar(32) COMMENT 'Device phone category',
    PRIMARY KEY (device_id,date_ini)
) COMMENT 'Table storing states for SCD type 2 attributes of the tacs'
;
  
CREATE TABLE gbic_global_dims_geo_zones(
    gbic_op_id                  int               COMMENT 'Operator Global identifier',
    gbic_geo_zone_id            int               COMMENT 'Global geographic zone ',
    loc_lev_7                   varchar(128)      COMMENT 'Location name of L7. Maximum aggregation',
    loc_lev_6                   varchar(128)      COMMENT 'Location name of L6',
    loc_lev_5                   varchar(128)      COMMENT 'Location name of L5',
    loc_lev_4                   varchar(128)      COMMENT 'Location name of L4. Minimum aggregation',
    activation_date             date              COMMENT 'Activation date of the customer',
    deactivation_date           date              COMMENT 'Month when a customer leaves',
    PRIMARY KEY (gbic_op_id,gbic_geo_zone_id)
) COMMENT 'Table storing customers(Not residential) snapshot by month. Includes SCD type 1 attributes of the customers'
;

CREATE TABLE gbic_global_dims_m_tariffs(
    gbic_op_id          int         COMMENT 'Operator Global identifier',
    gbic_tariff_id      int         COMMENT 'Global tariff plan id',
    tariff_plan_id      varchar(16) COMMENT 'Operator Tariff plan code',
    activation_date     date        COMMENT 'Month when a tariff is added',
    deactivation_date   date        COMMENT 'Month when a tariff is deleted',
    PRIMARY KEY (gbic_op_id,gbic_tariff_id)
) COMMENT 'Table storing tariffs snapshot by month'
;

CREATE TABLE gbic_global_dims_m_tariffs_history(
    gbic_op_id         int          COMMENT 'Operator Global identifier',
    gbic_tariff_id     int          COMMENT 'Global tariff plan id',
    date_ini           date         COMMENT 'Initial date of the state',
    date_end           date         COMMENT 'End date of the state',
    des_plan           varchar(128) COMMENT 'Name of the tariff plan',
    is_data_tariff     int          COMMENT 'Tariff of type data or not. 0->Yes, 1->No',
    PRIMARY KEY (gbic_op_id,gbic_tariff_id,date_ini)
) COMMENT 'Table storing states for SCD type 2 attributes of tariffs'
;

CREATE TABLE gbic_global_dims_customers(
    gbic_op_id                  int             COMMENT 'Operator Global identifier',
    gbic_customer_id            int             COMMENT 'Global customer id',
    customer_id                 varchar(32)     COMMENT 'Operator Customer Identifier',
    ob_id                       varchar(1)      COMMENT 'Identify the ob providing the service to the customer Mobile,Fix,Convergent',
    party_identification_num    varchar(64)     COMMENT 'Customer Fiscal identifier',
    creation_date               date            COMMENT 'Creation date of the organization',
    activation_date             date            COMMENT 'Activation date of the customer',
    deactivation_date           date            COMMENT 'Month when a customer leaves',
    PRIMARY KEY (gbic_op_id,gbic_customer_id)
) COMMENT 'Table storing customers(Not residential) snapshot by month. Includes SCD type 1 attributes of the customers'
;

CREATE TABLE gbic_global_dims_customers_history(
    gbic_op_id          int             COMMENT 'Operator Global identifier',
    gbic_customer_id    int             COMMENT 'Global customer id',
    date_ini            date            COMMENT 'Initial date of the state',
    date_end            date            COMMENT 'End date of the state',
    seg_global_id       int             COMMENT 'Global Organization Segment id for the customer. See table GBIC_GLOBAL_STD_LOCAL_CONCEPT_MAPPINGS->concept_id=1',
    org_name            varchar(256)    COMMENT 'Name of the organization',
    PRIMARY KEY (gbic_op_id,gbic_customer_id,date_ini)
) COMMENT 'Table storing states for SCD type 2 attributes of the customers'
;
