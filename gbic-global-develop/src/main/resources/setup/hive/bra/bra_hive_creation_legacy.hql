CREATE DATABASE GBIC_GLOBAL;

USE GBIC_GLOBAL;


-- **************************************
-- TABLES WITH DETAILED SOURCE DATA: BRA
-- **************************************
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_CUSTOMER;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_CUSTOMER;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_DIRECAO_CHAMADA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_MOV_TYPE;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_PLATAFORMA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_PLNO;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_REGION;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_SEGMENT;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_SENTIDO_CHAMADA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_SENTIDO_COBRANCA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_SERVICES;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_SIST_PAGAMENTO;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_SITU_CHAMADO_CHAMADOR;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_TIPO_COBRANCA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_DIM_TIPO_TRAFEGO;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_FATURA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_INF_LINE_DEVICE;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_INTERCON;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_LINE_SERVICES;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_MOV_DEV;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_RECARGA;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_TRAFEGO;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_TRAFEGO_DADOS;
DROP TABLE IF EXISTS GBIC_GLOBAL_BRA_VENTAS_IMEI;

CREATE TABLE GBIC_GLOBAL_BRA_CUSTOMER(
    gbic_op_name             string,
    id_month                 string,
    id_cli                   bigint,
    customer_id_mov          bigint,
    customer_id_fjo          bigint,
    id_tipo_carteira         int,
    ds_grpo_sgmt_cli         string,
    ds_oprc                  string,
    ds_prsc                  string,
    ds_grpo_prdt_cli_pre     string,
    ds_sub_grpo_prdt_cli_pre string,
    id_sexo                  string,
    dt_alta_cli              string,
    ds_agng_cli              string,
    dt_nscm                  string,
    ds_clss_cnae             string,
    qt_cli                   string,
    qt_lnha_mvel_pos         int,
    qt_lnha_mvel_pre         int,
    qt_lnha_mvel_cntl        int,
    qt_lnha_fwt_pos          int,
    qt_lnha_fwt_pre          int,
    qt_lnha_fwt_cntl         int,
    qt_lnha_fixa_pos         int,
    qt_lnha_fixa_pre         int,
    qt_tv_dth_sd             int,
    qt_tv_dth_hd             int,
    qt_tv_fbra_sd            int,
    qt_tv_fbra_hd            int,
    qt_tv_cabo_sd            int,
    qt_tv_cabo_hd            int,
    qt_vivo_play             int,
    qt_bnda_lrga_fbra        int,
    qt_bnda_lrga_cabo        int,
    qt_bnda_lrga_adsl        int,
    qt_bnda_lrga_vdsl        int,
    qt_plca_3g_pos           int,
    qt_plca_3g_pre           int,
    qt_plca_4g_pos           int,
    qt_plca_4g_pre           int,
    qt_plca_fwt_pos          int,
    qt_plca_fwt_pre          int,
    qt_plca_fwt_cntl         int,
    qt_m2m_3g                int,
    qt_m2m_4g                int,
    qt_pdti                  int,
    qt_prqe_ddos             int,
    qt_jntr_tlsp             int,
    qt_rmal_tlsp             int,
    qt_trnc_tlsp             int,
    qt_a_tlcm_ada            int,
    qt_a_tlcm_vox            int,
    qt_a_tlcm_e1             int,
    qt_rcrg                  int
) PARTITIONED BY (
    gbic_op_id               int,
    month                    string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_CUSTOMER(
    gbic_op_name     string,
    id_cli           bigint,
    customer_id_mov  bigint,
    customer_id_fjo  bigint,
    assinatura       bigint,
    nrc_prnc         bigint,
    codigo_postal    int,
    ds_grau_esco     string,
    socio_econ       string,
    ds_estd_cvil     string,
    fecha_actlz      string
) PARTITIONED BY (
    gbic_op_id       int,
    month            string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_DIRECAO_CHAMADA(
    gbic_op_name       string,
    cd_direcao_chamada int,
    ds_direcao_chamada string
) PARTITIONED BY (
    gbic_op_id         int,
    month              string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_MOV_TYPE(
    gbic_op_name string,
    id_mov       int,
    ds_mov       string
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_PLATAFORMA(
    gbic_op_name string,
    id_pltf       int,
    des_pltf      string
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_PLNO(
    gbic_op_name      string,
    id_plno           int,
    cd_plno_sist_orig string,
    ds_plno           string,
    ds_pltf           string,
    nm_unde_rgnl      string,
    ds_tipo_crtr      string,
    dt_ini_cmcl_plno  string,
    dt_fim_cmcl_plno  string,
    qt_frqa           int,
    ds_tipo_plno      string,
    ds_tipo_plan_seg  string,
    classe_plan_seg   string,
    tit_dep           string,
    dt_atlz_dw        string
) PARTITIONED BY (
    gbic_op_id        int,
    month             string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_REGION(
    gbic_op_name string,
    cod_region   int,
    des_region   string
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_SEGMENT(
    gbic_op_name string,
    id_segment  int,
    des_segment string
) PARTITIONED BY (
    gbic_op_id  int,
    month       string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_SENTIDO_CHAMADA(
    gbic_op_name       string,
    cd_sentido_chamada int,
    ds_sentido_chamada string
) PARTITIONED BY (
    gbic_op_id         int,
    month              string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_SENTIDO_COBRANCA(
    gbic_op_name        string,
    cd_sentido_cobranca int,
    ds_sentido_cobranca string
) PARTITIONED BY (
    gbic_op_id          int,
    month               string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_SERVICES(
    gbic_op_name     string,
    id_srvc             int,
    cd_srvc             string,
    ds_srvc             string,
    dt_ini_srvc         string,
    dt_fim_srvc         string,
    ds_clsf_srvc        string,
    ds_tipo_srvc        string,
    nm_unde_rgnl        string,
    ds_pltf             string,
    nm_sist_orig        string,
    qt_frqa             string,
    ds_unde_mdda_ftrm   string,
    ds_tipo_cntr_fdld   string,
    ds_tipo_crtr        string,
    ds_fmla_srvc        string,
    ds_ctga_srvc        string,
    ds_grpo_srvc        string,
    ds_sub_grpo_srvc    string,
    ds_sub_grpo_pf_srvc string,
    pntc_pj             double,
    pntc_pf             double,
    dt_insr_dw          string,
    dt_atlz_dw          string,
    fl_pct_extra        int,
    prco_bruto          double,
    prco_liquido        double
) PARTITIONED BY (
    gbic_op_id       int,
    month            string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_SIST_PAGAMENTO(
    gbic_op_name      string,
    id_sist_pagamento int,
    ds_sist_pagamento string
) PARTITIONED BY (
    gbic_op_id        int,
    month             string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_SITU_CHAMADO_CHAMADOR(
    gbic_op_name             string,
    cd_situ_chamado_chamador int,
    ds_situ_chamado_chamador string
) PARTITIONED BY (
    gbic_op_id               int,
    month                    string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_TIPO_COBRANCA(
    gbic_op_name     string,
    cd_tipo_cobranca int,
    ds_tipo_cobranca string
) PARTITIONED BY (
    gbic_op_id       int,
    month            string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_DIM_TIPO_TRAFEGO(
    gbic_op_name    string,
    cd_tipo_trafego int,
    ds_tipo_trafego string
) PARTITIONED BY (
    gbic_op_id      int,
    month           string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_FATURA(
    gbic_op_name string,
    id_month          string,
    msisdn            bigint,
    customer_id       int,
    fecha_inicio_fact string,
    fecha_final_fact  string,
    agregacion_fact   string,
    id_factura        int,
    segundos_llamadas int,
    numero_llamadas   int,
    numero_byte_total bigint,
    ingresos_total    double,
    ingresos_neto     double
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_INF_LINE_DEVICE(
    gbic_op_name      string,
    id_month          string,
    msisdn            bigint,
    customer_id       int,
    fl_multivivo      int,
    upselling         string,
    tp_terminal       string,
    imei              bigint,
    segment_id        int,
    age_id            string,
    id_pltf           int,
    des_pltf          string,
    line_status       int,
    id_plan           int,
    uf_state          string,
    cod_region        int,
    activation_date   string,
    nr_cep            int,
    qt_calls          int,
    id_sist_pagamento int,
    ds_sist_pagamento string,
    id_tipo_carteira  int,
    tp_mtrl           string,
    fl_prqe_int       int,
    id_fatura         int,
    valida_plan       string,
    tit_dep           string,
    tipo_plan         string,
    classe_plan       string,
    frqa_pl_int       string,
    frqa_tot_ftra     int,
    id_segm_negocio   int,
    nr_tlfn           bigint
) PARTITIONED BY (
    gbic_op_id        int,
    month             string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_INTERCON(
    gbic_op_name             string,
    id_month                 string,
    msisdn                   bigint,
    cd_tipo_cobranca         int,
    cd_sentido_chamada       int,
    cd_sentido_cobranca      int,
    cd_tipo_trafego          int,
    cd_direcao_chamada       int,
    cd_situ_chamado_chamador int,
    ds_tipo_cobranca         string,
    ds_sentido_chamada       string,
    ds_sentido_cobranca      string,
    ds_tipo_trafego          string,
    ds_direcao_chamada       string,
    ds_situ_chamado_chamador string,
    qt_seg_real              int,
    qt_seg_tarifado          int,
    qt_chamada_intercon      int,
    vl_bruto_intercon        double,
    vl_tarifa_intercon       double,
    vl_liquido_intercon      double,
    vl_imposto_intercon      double
) PARTITIONED BY (
    gbic_op_id               int,
    month                    string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_LINE_SERVICES(
    gbic_op_name string,
    id_month     string,
    msisdn       bigint,
    id_service   int
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_MOV_DEV(
    gbic_op_name         string,
    id_month             string,
    msisdn               bigint,
    customer_id          int,
    id_mov               int,
    ds_mov               string,
    mov_channel          string,
    mov_date             string,
    fl_portabilidade     int,
    numero_do_telefone   bigint,
    chave_linha_anterior bigint,
    situacao_inicial     int,
    situacao_final       int
) PARTITIONED BY (
    gbic_op_id           int,
    month                string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_RECARGA(
    gbic_op_name         string,
    id_month             string,
    msisdn               bigint,
    customer_id          int,
    ingresos_recarga     double,
    num_recarga          int
) PARTITIONED BY (
    gbic_op_id          int,
    month               string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_TRAFEGO(
    gbic_op_name                 string,
    id_month                     string,
    msisdn                       bigint,
    id_day_type                  int,
    id_time_slot                 int,
    id_tipo_origem               int,
    vl_franquia_voz_out          double,
    vl_franquia_voz_in           double,
    seg_llamadas_bonus_out       double,
    seg_llamadas_bonus_in        double,
    seg_llamadas_franquia_out    double,
    seg_llamadas_franquia_in     double,
    num_llamadas_franquia_out    int,
    num_llamadas_franquia_in     int,
    seg_llamadas_out             double,
    seg_llamadas_in              double,
    num_llamadas_out             int,
    num_llamadas_in              int,
    ingresos_liquido_llamada_out double,
    ingresos_liquido_llamada_in  double,
    ingresos_total_llamada_out   double,
    ingresos_total_llamada_in    double,
    num_franquia_sms_out         int,
    num_franquia_sms_in          int,
    num_sms_out                  int,
    num_sms_in                   int,
    ingresos_sms_out             double,
    ingresos_sms_in              double,
    num_franquia_mms_out         int,
    num_franquia_mms_in          int,
    num_mms_out                  int,
    num_mms_in                   int,
    ingresos_mms_out             double,
    ingresos_mms_in              double,
    num_byte_franquia_out        int,
    num_byte_franquia_in         int,
    num_byte_total_out           int,
    num_byte_total_in            int
) PARTITIONED BY (
    gbic_op_id                   int,
    month                        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_TRAFEGO_DADOS(
    gbic_op_name       string,
    mes                 string,
    customer_id         int,
    msisdn              bigint,
    id_sist_pgto        int,
    ds_sist_pgto        string,
    tipo_linea          string,
    id_day_type         int,
    id_time_slot        int,
    qt_mbyte_total      int,
    num_sessiones_datos int
) PARTITIONED BY (
    gbic_op_id          int,
    month               string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_BRA_VENTAS_IMEI(
    gbic_op_name      string,
    id_lnha           bigint,
    cd_area_rgst      int,
    dt_mvmt_lnha      string,
    id_pltf           int,
    ds_pltf           string,
    id_tipo_mvmt_lnha int,
    ds_tipo_mvmt_lnha string,
    id_area_rgnl      int,
    nm_area_rgnl      string,
    id_tipo_crtr      int,
    ds_tipo_crtr      string,
    gnro_vndr         string,
    cep               string,
    rede              string,
    ds_plno           string,
    sgmt_plno         string,
    ap_cd_cnl_dstr    int,
    ap_nm_cnl_dstr    string,
    ap_nr_sral        bigint,
    ap_vl_nota_fscl   double,
    nome_comercial    string,
    tecnologia        string
) PARTITIONED BY (
    gbic_op_id          int,
    month               string)
STORED AS ORC;


-- **************************************
-- TABLES WITH AGGREGATED DATA: BRA
-- **************************************
