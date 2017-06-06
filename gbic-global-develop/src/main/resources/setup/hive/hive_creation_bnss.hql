CREATE DATABASE IF NOT EXISTS {{ project.prefix }}gbic_global_bnss;

USE {{ project.prefix }}gbic_global_bnss;

-- ***************************
-- TABLES WITH AGGREGATED DATA
-- ***************************
DROP TABLE IF EXISTS kpis_mobile;
DROP TABLE IF EXISTS kpis_fix;
DROP TABLE IF EXISTS kpis_tariff_dynamics_agg_b2b;
DROP TABLE IF EXISTS kpis_tariff_dynamics_agg_b2c;

CREATE TABLE kpis_mobile (
    gbic_op_id           int    COMMENT 'Operator Global identifier',
    seg_global_id        int    COMMENT 'Organization Segment Identifier.See table GBIC_GLOBAL_STD_GLOBAL_CONCEPTS->concept_id=1',
    pre_post_id          string COMMENT 'P->Prepaid, C->Contract, X->Hybrid',
    gbic_customer_id     int    COMMENT 'Global Customer Identifier. See tables GBIC_GLOBAL_DIMS_CUSTOMERS',
    device_id            int    COMMENT 'Tac Identifier. See tables GBIC_GLOBAL_DIMS_TACS',
    gbic_tariff_id       int    COMMENT 'Global Tariff Identifier. See table GBIC_GLOBAL_DIMS_TARIFFS',
    gbic_geo_zone_id     int    COMMENT 'Identifier of geo position',
    months_old           int    COMMENT 'Groups of months since activation', 
    prod_type_cd         string COMMENT 'Prod type indicator',
    bta_ind              int    COMMENT 'Indicator about if lines are stable according of OB criteria',
    multisim_ind         int    COMMENT 'Indicator about multisim. 1 -> Multisim line but not the main. 0 -> Main lines being or not multisim',
    month                string COMMENT 'Month in format YYYY-MM-DD',
    n_lines_total        bigint COMMENT '# total of lines',
    n_lines_exceed       bigint COMMENT '# of lines exceeding their allowance',
    n_lines_data_tariff  bigint COMMENT '# of lines which have a data tariff',
    n_lines_extra_data   bigint COMMENT '# of lines buying extras',
    n_lines_exceed_extra bigint COMMENT '# of lines exceeding their allowance and buying extras',
    vl_data_bundled      double COMMENT '# Mb of data bundled',
    vl_data_exceed       double COMMENT '# Mb of data consumed over the bundled ones by users exceeding the allowance',
    vl_data_consumed     double COMMENT '# Mb of data consumed',
    vl_voice_consumed    double COMMENT '# Minutes of voice consumed',
    vl_sms_consumed      double COMMENT '# Sms consumed',
    n_voice_calls        double COMMENT '# Voice calls',
    quota_agg_rv         double COMMENT 'Revenues from quota not in data voice and mess',
    quota_data_rv        double COMMENT 'Revenues from quota data',
    quota_voice_rv       double COMMENT 'Revenues from quota voice',
    quota_mess_rv        double COMMENT 'Revenues from quota mess',
    traffic_agg_rv       double COMMENT 'Revenues from traffic not in data voice and mess',
    traffic_data_rv      double COMMENT 'Revenues from traffic data',
    traffic_voice_rv     double COMMENT 'Revenues from traffic voice',
    traffic_mess_rv      double COMMENT 'Revenues from traffic mess',
    roaming_rv           double COMMENT 'Revenues from roaming',
    sva_rv               double COMMENT 'Revenue from value-added services',
    packs_rv             double COMMENT 'Income from hired packages',
    top_up_ex_rv         double COMMENT 'Income balances expired',
    top_up_co_rv         double COMMENT 'Income consumption refills',
    gb_camp_rv           double COMMENT 'Returns and campaigns',
    others_rv            double COMMENT 'Other income not fall into any classification',
    tot_rv               double COMMENT 'total revenue without top ups',
    top_up_rv            double COMMENT 'Total amount recharged',
    itx_rv               double COMMENT 'Interconnection revenues (voice + sms)',
    exp_itx_rv           double COMMENT 'Interconnection expenses (voice + sms)',
    extra_data_rv        double COMMENT 'Revenues from extra data packages from m_lines',
    total_invoice_rv     double COMMENT 'Sum of total revenue billed to the line'
) COMMENT 'Table storing generic kpis about mobile business'
PARTITIONED BY (
    gbic_op_id_pt        int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt             string COMMENT 'Partitioning by Month in format YYYY-MM-DD',
    seg_global_id_pt     int    COMMENT 'Partitioning by segment to improve B2C and B2B queries'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE kpis_fix (
    gbic_op_id           int    COMMENT 'Operator Global identifier',
    seg_global_id        int    COMMENT 'Organization Segment Identifier.See table GBIC_GLOBAL_STD_GLOBAL_CONCEPTS->concept_id=1',
    gbic_customer_id     int    COMMENT 'Global Customer Identifier. See tables GBIC_GLOBAL_DIMS_CUSTOMERS',
    bband_ind            int    COMMENT 'If has bbam',
    gbic_bband_tariff_id string COMMENT 'Global BBAND Tariff Identifier. See table GBIC_GLOBAL_DIMS_TARIFFS',
    gbic_voice_tariff_id string COMMENT 'Global VOICE Tariff Identifier. See table GBIC_GLOBAL_DIMS_TARIFFS',
    gbic_tv_tariff_id    string COMMENT 'Global TV Tariff Identifier. See table GBIC_GLOBAL_DIMS_TARIFFS',
    gbic_geo_zone_id     int    COMMENT 'Identifier of geo position',
    months_old           int    COMMENT 'Groups of months since activation',
    bband_type_cd        string COMMENT 'Type of bband',
    speed_band_qt        string COMMENT 'Speed of bband',
    month                string COMMENT 'Month in format YYYY-MM-DD',
    n_lines_total        bigint COMMENT '# total of lines',
    bband_month_rv       double COMMENT 'Total invoice revenues',
    total_month_rv       double COMMENT 'Total revenues from voice,tv and bam'
) COMMENT 'Table storing generic kpis about mobile business'
PARTITIONED BY (
    gbic_op_id_pt    int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt         string COMMENT 'Partitioning by Month in format YYYY-MM-DD',
    seg_global_id_pt int    COMMENT 'Partitioning by segment to improve B2C and B2B queries'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE kpis_tariff_dynamics_agg_b2b (
    seg_global_id        int    COMMENT 'Global organizational segment identifier',
    seg_global_name      string COMMENT 'Global organizational segment name',
    tariff_plan_id_prev  string COMMENT 'Previous month tariff plan id',
    tariff_plan_des_prev string COMMENT 'Previous month tariff plan description',
    tariff_plan_id_curr  string COMMENT 'Current month tariff plan id',
    tariff_plan_des_curr string COMMENT 'Current month tariff plan description',    
    movement_local_cd    string COMMENT 'Local movement type group identifier code', 
    movement_local_name  string COMMENT 'Local movement type group name', 
    movement_global_id   int    COMMENT 'Global movement type group identifier', 
    movement_global_name string COMMENT 'Global movement type group name',
    num_lines            int    COMMENT 'Number of changed lines',
    total_rv_prev        double COMMENT 'Total revenues previous month',
    total_rv_curr        double COMMENT 'Total revenues current month',
    percen05_prev        double COMMENT 'Percentile  5 revenues previous month',
    percen25_prev        double COMMENT 'Percentile 25 revenues previous month',
    percen50_prev        double COMMENT 'Percentile 50 revenues previous month',
    percen75_prev        double COMMENT 'Percentile 75 revenues previous month',
    percen95_prev        double COMMENT 'Percentile 95 revenues previous month',
    percen05_curr        double COMMENT 'Percentile  5 revenues current month',
    percen25_curr        double COMMENT 'Percentile 25 revenues current month',
    percen50_curr        double COMMENT 'Percentile 50 revenues current month',
    percen75_curr        double COMMENT 'Percentile 75 revenues current month',
    percen95_curr        double COMMENT 'Percentile 95 revenues current month',
    type                 string COMMENT 'Type of movement: REPO, CHURNER, PERMANENT, NEW_COMMER'
) COMMENT 'Tariff dynamics for business grouped by segment'
PARTITIONED BY (
    gbic_op_id           int    COMMENT 'GBIC GLOBAL OPERATOR ID',
    month                string COMMENT 'Date of monthly files',
    status               string COMMENT 'Line status: ACTIVE, INACTIVE'
);

CREATE TABLE kpis_tariff_dynamics_agg_b2c (
    seg_global_id        int    COMMENT 'Global organizational segment identifier',
    seg_global_name      string COMMENT 'Global organizational segment name',
    tariff_plan_id_prev  string COMMENT 'Previous month tariff plan id',
    tariff_plan_des_prev string COMMENT 'Previous month tariff plan description',
    tariff_plan_id_curr  string COMMENT 'Current month tariff plan id',
    tariff_plan_des_curr string COMMENT 'Current month tariff plan description',
    movement_local_cd    string COMMENT 'Local movement type group identifier code', 
    movement_local_name  string COMMENT 'Local movement type group name', 
    movement_global_id   int    COMMENT 'Global movement type group identifier', 
    movement_global_name string COMMENT 'Global movement type group name',
    num_lines            int    COMMENT 'Number of changed lines',
    total_rv_prev        double COMMENT 'Total revenues previous month',
    total_rv_curr        double COMMENT 'Total revenues current month',
    percen05_prev        double COMMENT 'Percentile  5 revenues previous month',
    percen25_prev        double COMMENT 'Percentile 25 revenues previous month',
    percen50_prev        double COMMENT 'Percentile 50 revenues previous month',
    percen75_prev        double COMMENT 'Percentile 75 revenues previous month',
    percen95_prev        double COMMENT 'Percentile 95 revenues previous month',
    percen05_curr        double COMMENT 'Percentile  5 revenues current month',
    percen25_curr        double COMMENT 'Percentile 25 revenues current month',
    percen50_curr        double COMMENT 'Percentile 50 revenues current month',
    percen75_curr        double COMMENT 'Percentile 75 revenues current month',
    percen95_curr        double COMMENT 'Percentile 95 revenues current month',
    type                 string COMMENT 'Type of movement: REPO, CHURNER, PERMANENT, NEW_COMMER'
) COMMENT 'Tariff dynamics for business grouped by segment'
PARTITIONED BY (
    gbic_op_id           int    COMMENT 'GBIC GLOBAL OPERATOR ID',
    month                string COMMENT 'Date of monthly files',
    status               string COMMENT 'Line status: ACTIVE, INACTIVE'
);


-- **************************************
-- TABLES WITH DIMENSIONS
-- **************************************
DROP TABLE IF EXISTS dims_f_tariffs;
DROP TABLE IF EXISTS dims_f_tariffs_history;
DROP TABLE IF EXISTS dims_geo_zones;
DROP TABLE IF EXISTS dims_tacs;
DROP TABLE IF EXISTS dims_tacs_history;
DROP TABLE IF EXISTS dims_m_tariffs;
DROP TABLE IF EXISTS dims_m_tariffs_history;
DROP TABLE IF EXISTS dims_customers;
DROP TABLE IF EXISTS dims_customers_history; 

CREATE TABLE dims_f_tariffs (
    gbic_op_id        int    COMMENT 'Operator Global identifier',
    gbic_tariff_id    int    COMMENT 'Global tariff plan id',
    tariff_plan_id    string COMMENT 'Operator Tariff plan code',
    activation_date   string COMMENT 'Month when a tariff is added',
    deactivation_date string COMMENT 'Month when a tariff is deleted'
) COMMENT 'Table storing Fix tariffs snapshot by month'
PARTITIONED BY (
    gbic_op_id_pt     int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt          string COMMENT 'Month in format YYYY-MM-DD',
    hist_flag         string COMMENT 'E->Existing, N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_f_tariffs_history (
    gbic_op_id     int    COMMENT 'Operator Global identifier',
    gbic_tariff_id int    COMMENT 'Global tariff plan id',
    date_ini       string COMMENT 'Initial date of the state',
    date_end       string COMMENT 'End date of the state',
    des_plan       string COMMENT 'Name of the tariff plan'
) COMMENT 'Table storing states for SCD type 2 attributes of Fix tariffs'
PARTITIONED BY (
    gbic_op_id_pt  int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt       string COMMENT 'Month in format YYYY-MM-DD',  
    hist_flag      string COMMENT 'R->Remained, S->Stored,U->Updated,N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_geo_zones (
    gbic_op_id        int    COMMENT 'Operator Global identifier',
    gbic_geo_zone_id  int    COMMENT 'Global geographic zone ',
    loc_lev_7         string COMMENT 'Location name of L7. Maximum aggregation',
    loc_lev_6         string COMMENT 'Location name of L6',
    loc_lev_5         string COMMENT 'Location name of L5',
    loc_lev_4         string COMMENT 'Location name of L4. Minimum aggregation',
    activation_date   string COMMENT 'Activation date of the customer',
    deactivation_date string COMMENT 'Month when a customer leaves'
) COMMENT 'Table storing customers(Not residential) snapshot by month. Includes SCD type 1 attributes of the customers'
PARTITIONED BY (
    gbic_op_id_pt     int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt          string COMMENT 'Month in format YYYY-MM-DD',
    hist_flag         string COMMENT 'E->Existing, N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_tacs (
    device_id         int    COMMENT 'Tac id',
    des_manufact      string COMMENT 'Device manufacturer',
    des_model         string COMMENT 'Device model',
    os                string COMMENT 'Device OS',
    version_os        string COMMENT 'Version of the OS',
    technology        string COMMENT '4G,3G,2G',
    activation_date   string COMMENT 'Month when a device is added',
    deactivation_date string COMMENT 'Month when a device is deleted'
) COMMENT 'Table storing tacs snapshot by month. Includes SCD type 1 attributes of the tacs'
PARTITIONED BY (
    month_pt          string COMMENT 'Month in format YYYY-MM-DD'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_tacs_history (
    device_id       int    COMMENT 'Tac id',
    date_ini        string COMMENT 'Initial date of the state',
    date_end        string COMMENT 'End date of the state',
    market_category string COMMENT 'Device market category',
    tef_category    string COMMENT 'Device phone category'
) COMMENT 'Table storing states for SCD type 2 attributes of the tacs'
PARTITIONED BY (
    month_pt        string COMMENT 'Month in format YYYY-MM-DD', 
    hist_flag       string COMMENT 'U->Unchanged, C->Closed, N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_m_tariffs (
    gbic_op_id        int    COMMENT 'Operator Global identifier',
    gbic_tariff_id    int    COMMENT 'Global tariff plan id',
    tariff_plan_id    string COMMENT 'Operator Tariff plan code',
    activation_date   string COMMENT 'Month when a tariff is added',
    deactivation_date string COMMENT 'Month when a tariff is deleted'
) COMMENT 'Table storing Mobile tariffs snapshot by month'
PARTITIONED BY (
    gbic_op_id_pt     int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt          string COMMENT 'Month in format YYYY-MM-DD',
    hist_flag         string COMMENT 'E->Existing, N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_m_tariffs_history (
    gbic_op_id     int    COMMENT 'Operator Global identifier',
    gbic_tariff_id int    COMMENT 'Global tariff plan id',
    date_ini       string COMMENT 'Initial date of the state',
    date_end       string COMMENT 'End date of the state',
    des_plan       string COMMENT 'Name of the tariff plan',
    is_data_tariff int    COMMENT 'Tariff of type data or not. 0->Yes, 1->No'
) COMMENT 'Table storing states for SCD type 2 attributes of Mobile tariffs'
PARTITIONED BY (
    gbic_op_id_pt  int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt       string COMMENT 'Month in format YYYY-MM-DD',  
    hist_flag      string COMMENT 'R->Remained, S->Stored,U->Updated,N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_customers (
    gbic_op_id               int    COMMENT 'Operator Global identifier',
    gbic_customer_id         int    COMMENT 'Global customer id',
    customer_id              string COMMENT 'Operator Customer Identifier',
    ob_id                    string COMMENT 'Identify the ob providing the service to the customer Mobile,Fix,Convergent',
    party_identification_num string COMMENT 'Customer Fiscal identifier',
    creation_date            string COMMENT 'Creation date of the organization',
    activation_date          string COMMENT 'Activation date of the customer',
    deactivation_date        string COMMENT 'Month when a customer leaves'
) COMMENT 'Table storing customers(Not residential) snapshot by month. Includes SCD type 1 attributes of the customers'
PARTITIONED BY (
    gbic_op_id_pt            int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt                 string COMMENT 'Month in format YYYY-MM-DD',
    hist_flag                string COMMENT 'E->Existing, N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;

CREATE TABLE dims_customers_history (
    gbic_op_id          int    COMMENT 'Operator Global identifier',
    ob_id               string COMMENT 'Operator Customer Identifier',
    gbic_customer_id    int    COMMENT 'Global customer id',
    date_ini            string COMMENT 'Initial date of the state',
    date_end            string COMMENT 'End date of the state',
    seg_global_id       int    COMMENT 'Global Organization Segment id for the customer. See table GBIC_GLOBAL_STD_LOCAL_CONCEPT_MAPPINGS->concept_id=1',
    org_name            string COMMENT 'Name of the organization'
) COMMENT 'Table storing states for SCD type 2 attributes of the customers'
PARTITIONED BY (
    gbic_op_id_pt       int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt            string COMMENT 'Month in format YYYY-MM-DD', 
    hist_flag           string COMMENT 'R->Remained, S->Stored,U->Updated,N->New records in the month'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  STORED AS TEXTFILE;


-- **************************************
-- EXTERNAL TABLES
-- **************************************
DROP TABLE IF EXISTS dims_corporates;

CREATE TABLE dims_corporates (
    id            int    COMMENT 'Corporate ID',
    mnc_name      string COMMENT 'Generic Name',
    client_cif    string COMMENT 'CIF of client',
    client_name   string COMMENT 'Name of client',
    group_cif     string COMMENT 'CIF of group',
    group_name    string COMMENT 'Name of group',
    perimeter     string COMMENT 'Flag of corporate'
) COMMENT 'Table storing corporate information'
PARTITIONED BY (
    gbic_op_id_pt int    COMMENT 'Partitioning by Operator Global identifier',
    month_pt      string COMMENT 'Month in format YYYY-MM-DD'
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\;'
  STORED AS TEXTFILE;


-- **************************************
-- DASHBOARD's TABLES
-- **************************************
DROP TABLE IF EXISTS report_b2c_bta;

CREATE TABLE report_b2c_bta (
    gbic_op_id           int    COMMENT 'Operator Global Name',
    month                string COMMENT 'Month',
    bta_ind              int    COMMENT 'Beyond Allowance Indicator',
    loc_lev_7            string COMMENT 'Commercial distrit',
    prod_type_cd         string COMMENT 'Prod type indicator',
    gbic_tariff_id       int    COMMENT 'Global Tariff Identifier. See table GBIC_GLOBAL_DIMS_TARIFFS',
    is_data_tariff       string COMMENT 'Indicator of data plan',
    des_plan             string COMMENT 'Description of plan',
    des_manufact         string COMMENT 'Description of manufact',
    des_model            string COMMENT 'Device Model',
    os                   string COMMENT 'System operator',
    technology           string COMMENT 'Technology',
    market_category      string COMMENT 'Market category (smartphonetablet…)',
    months_old           int    COMMENT 'Groups of months since activation',
    n_lines_total        int    COMMENT '# total of lines include multisim',
    n_lines_princial     int    COMMENT '# total of lines principal',
    n_lines_exceed       int    COMMENT '# of lines exceeding their allowance',
    n_lines_data_tariff  int    COMMENT '# of lines which have a data tariff',
    n_lines_extra_data   int    COMMENT '# of lines buying extras',
    n_lines_exceed_extra int    COMMENT '# of lines exceeding their allowance and buying extras',
    n_voice_calls        int    COMMENT '# Voice calls',
    vl_data_bundled      double COMMENT '# Mb of data bundled',
    vl_data_exceed       double COMMENT '# Mb of data consumed over the bundled ones by users exceedingthe allowance',
    vl_data_consumed     double COMMENT '# Mb of data consumed',
    vl_voice_consumed    double COMMENT '# Minutes of voice consumed',
    vl_sms_consumed      double COMMENT '# Sms consumed',
    quota_agg_rv         double COMMENT 'Revenues from quota not in data voice and mess',
    quota_data_rv        double COMMENT 'Revenues from quota data',
    quota_voice_rv       double COMMENT 'Revenues from quota voice',
    quota_mess_rv        double COMMENT 'Revenues from quota mess',
    traffic_agg_rv       double COMMENT 'Revenues from traffic not in data voice and mess',
    traffic_data_rv      double COMMENT 'Revenues from traffic data',
    traffic_voice_rv     double COMMENT 'Revenues from traffic voice',
    traffic_mess_rv      double COMMENT 'Revenues from traffic mess',
    roaming_rv           double COMMENT 'Revenues from roaming',
    sva_rv               double COMMENT 'Revenue from value-added services',
    packs_rv             double COMMENT 'Income from hired packages',
    top_up_ex_rv         double COMMENT 'Income balances expired',
    top_up_co_rv         double COMMENT 'Income consumption refills',
    gb_camp_rv           double COMMENT 'Returns and campaigns',
    others_rv            double COMMENT 'Other income not fall into any classification',
    tot_rv               double COMMENT 'total revenue without top ups',
    top_up_rv            double COMMENT 'Total amount recharged',
    itx_rv               double COMMENT 'Interconnection revenues (voice + sms)',
    exp_itx_rv           double COMMENT 'Interconnection expenses (voice + sms)',
    extra_data_rv        double COMMENT 'Revenues from extra data packages from m_lines',
    total_invoice_rv     double COMMENT 'Sum of total revenue billed to the line'
) COMMENT ''
PARTITIONED BY (
    gbic_op_id_pt        int,
    month_pt             string
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\;'
  STORED AS TEXTFILE;
