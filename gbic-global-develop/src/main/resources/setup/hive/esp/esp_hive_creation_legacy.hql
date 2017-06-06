CREATE DATABASE GBIC_GLOBAL;

USE GBIC_GLOBAL;

-- **************************************
-- TABLES WITH DETAILED SOURCE DATA: ESP
-- **************************************
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_ANTIG_TERM;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_CANJES;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_CONT_COMPROMISO;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_CONTRATOS;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_ESTADOS_LINEAS;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_IMEISHIST;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_INF_LIN_TERM1;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_INF_LINEA_VALOR_EM;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_INF_LINEA_VALOR;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_LINEAS_MULTISIM;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_LINEAS_SERVICIOS;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_MOV_DESC_TERM;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_MOV_TERM;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_MOVIMIENTOS_SERVICIOS;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_POBLACIONES;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_SEGMENTO_ORGANIZATIVO;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_SEGMENTOS_TERM;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_TERM_VOZDATOS1;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_TERM_VOZDATOS2;
DROP TABLE IF EXISTS GBIC_GLOBAL_ESP_TRAF_VOZ_HRC;


CREATE TABLE GBIC_GLOBAL_ESP_ANTIG_TERM (
    gbic_op_name string,
    id_dia_mes   string,
    msisdn       bigint,
    imei         bigint,
    fecha_alta   string
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_CANJES (
    gbic_op_name            string,
    id_dia_mes              string,
    msisdn                  bigint,
    id_cliente              bigint,
    id_dia_alta             string,
    fecha_ejecucion         string,
    imei                    bigint,
    tipo_canje_estrena      string,
    descripcion             string,
    importe_comision        double,
    imp_terminal            double,
    puntos_canjeados        double,
    importe_adicional_canje double,
    puntos_tras_canje       double,
    valor_punto             double
) PARTITIONED BY (
    gbic_op_id              int,
    month                   string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_CONT_COMPROMISO (
    gbic_op_name         string,
    id_dia_mes           string,
    msisdn               bigint,
    origen_contrato_cco  string,
    des_causa_alta       string,
    id_dia_firma         string,
    imei                 bigint,
    apoyo_economico      int,
    importe_penalizacion string,
    meses_permanencia    int
) PARTITIONED BY (
    gbic_op_id           int,
    month                string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_CONTRATOS (
    gbic_op_name            string,
    id_dia_mes              string,
    id_combinacion_contrato int,
    des_contrato            string,
    cjto_grupo_contrato     string
) PARTITIONED BY (
    gbic_op_id              int,
    month                   string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_ESTADOS_LINEAS (
    gbic_op_name    string,
    id_dia_mes      string,
    id_estado_linea int,
    estado_linea    string
) PARTITIONED BY (
    gbic_op_id      int,
    month           string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_IMEISHIST (
    gbic_op_name string,
    id_dia_mes   string,
    msisdn       bigint,
    cod_almacen  string,
    nom_almacen  string,
    cod_producto int,
    des_producto string,
    cod_swit     int,
    fecha_alta   string
) PARTITIONED BY (
    gbic_op_id   int,
    month        string,
    fortnight    string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_INF_LIN_TERM1 (
    gbic_op_name                  string,
    id_dia_mes                    string,
    id_cliente                    bigint,
    msisdn                        bigint,
    id_dia_alta                   string,
    cli_fecha_baja                string,
    id_combinacion_contrato       bigint,
    fecha_nacimiento              string,
    sexo                          string,
    id_identificador_segmentacion int,
    id_tipo_cliente_negocio       int,
    id_poblacion                  int,
    id_estado_linea               int,
    ind_pque_activo               string,
    ind_tipo_linea                string,
    imei                          bigint,
    importe_total_salida_mc       int,
    llamadas_salida_rc            int,
    llamadas_total_salida_mc      int,
    importe_total_entrada_mc      int,
    llamadas_total_entrada_mc     int,
    llamadas_crc                  int,
    llamadas_prog_puntos          int,
    llamadas_entrada_rgtr         int,
    llamadas_entrada_rc           int,
    seg_aire_entrada_rgtr         int,
    seg_aire_salida_rgtr_total    int,
    seg_aire_salida_rgtr_rc       int,
    seg_fact_salida_rc            int,
    llamadas_salida_rgtr_total    int,
    llamadas_entrada_inter        int,
    llamadas_salida_inter         int,
    seg_aire_entrada_inter        int,
    seg_aire_salida_inter         int,
    llamadas_entrada_fijo_tele    int,
    llamadas_salida_fijo_tele     int,
    seg_aire_entrada_fijo_tele    int,
    seg_aire_salida_fijo_tele     int,
    llamadas_entrada_fijo_otros   int,
    llamadas_salida_fijo_otros    int,
    seg_aire_entrada_fijo_otros   int,
    seg_aire_salida_fijo_otros    int,
    llamadas_entrada_otros_mov    int,
    llamadas_salida_otros_mov     int,
    seg_aire_entrada_otros_mov    int,
    seg_aire_salida_otros_mov     int,
    num_rec_coste_mes             int,
    num_rec_prom_mes              int,
    imp_rec_coste_mes             double,
    imp_rec_prom_mes              double,
    imp_saldo_mes                 double,
    vol_down_gprs                 bigint,
    vol_downlink_gprs_rc          bigint,
    vol_uplink_gprs               bigint,
    vol_uplink_gprs_rc            double,
    neto_facturacion              double,
    importe_actuaciones           double,
    mms_entrada                   bigint,
    mms_salida                    bigint,
    imp_mms_entrada               double,
    imp_mms_salida                double,
    eventos_premium               bigint,
    importe_premium               double,
    des_contrato                  string,
    cjto_grupo_contrato           string,
    estado_linea                  string,
    poblacion                     string,
    comunidad_autonoma            string,
    id_comunidad_autonoma         int,
    id_direccion_territorial      int,
    direccion_territorial         string,
    provincia                     string,
    id_provincia                  int,
    id_segmento_organizativo      int,
    segmento_organizativo         string,
    id_subsegmento_organizativo   int,
    subsegmento_organizativo      string,
    id_tramo_edad                 int,
    tramo_edad                    string,
    id_procedencia                int,
    procedencia                   string,
    id_fiabilidad                 int,
    fiabilidad                    string,
    des_segmento_valor            string,
    id_segmento                   int,
    des_segmento_negocio          string
) PARTITIONED BY (
    gbic_op_id                    int,
    month                         string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_INF_LINEA_VALOR_EM (
    gbic_op_name       string,
    id_dia_mes         string,
    msisdn             bigint,
    co_corporativo     int,
    cl_segmento        int,
    in_inactiva        int,
    im_compromiso      double,
    im_exencion        double,
    im_cuotas_voz_nac  double,
    im_cuotas_dat_nac  double,
    im_cuotas_mix_nac  double,
    im_cuotas_otr_nac  double,
    im_cuotas_voz_roa  double,
    im_cuotas_dat_roa  double,
    im_cuotas_mix_roa  double,
    im_cuotas_otr_roa  double,
    im_cuotas_voz_res  double,
    im_cuotas_dat_res  double,
    im_cuotas_mix_res  double,
    im_cuotas_otr_res  double,
    im_trafico_voz_nac double,
    im_trafico_dat_nac double,
    im_trafico_otr_nac double,
    im_trafico_voz_roa double,
    im_trafico_dat_roa double,
    im_trafico_otr_roa double,
    im_trafico_voz_res double,
    im_trafico_dat_res double,
    im_trafico_otr_res double,
    im_cuotas_nac      double,
    im_cuotas_roa      double,
    im_cuotas_res      double,
    im_trafico_nac     double,
    im_trafico_roa     double,
    im_trafico_res     double,
    im_varios_nac      double,
    im_varios_roa      double,
    im_varios_res      double,
    im_resto_no_clasif double,
    im_cuota_fusion    double,
    im_otros_fusion    double,
    to_linea           double
) PARTITIONED BY (
    gbic_op_id         int,
    month              string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_INF_LINEA_VALOR (
    gbic_op_name         string,
    id_dia_mes           string,
    msisdn               bigint,
    co_cliente           bigint,
    fx_dia_alta          string,
    in_con_pre           string,
    im_resto_no_clasif   double,
    im_cons_min          double,
    im_cons_min_dat      double,
    im_compromiso        double,
    im_voz               double,
    im_voz_r             double,
    im_datos             double,
    im_datos_r           double,
    im_eventos           double,
    im_mms               double,
    im_mms_r             double,
    im_sms_mms           double,
    im_sms               double,
    im_sms_r             double,
    im_recarga           double,
    im_resto             double,
    im_resto_r           double,
    im_trafico_mixto     double,
    im_trafico_mixto_r   double,
    im_fusion_voz        double,
    im_fusion_datos      double,
    im_ing_itx_resto_voz double,
    im_gas_itx_resto_voz double,
    im_ing_itx_tesa_voz  double,
    im_gas_itx_tesa_voz  double,
    im_ing_itx_resto_sms double,
    im_gas_itx_resto_sms double,
    im_ing_itx_tesa_sms  double,
    im_gas_itx_tesa_sms  double,
    to_linea             double
) PARTITIONED BY (
    gbic_op_id           int,
    month                string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_LINEAS_MULTISIM (
    gbic_op_name        string,
    id_dia_mes          string,
    msisdn              bigint,
    id_cliente          bigint,
    id_dia_alta         string,
    extension           bigint,
    max_id_dia_alta_ext string,
    min_id_dia_alta_ext string
) PARTITIONED BY (
    gbic_op_id          int,
    month               string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_LINEAS_SERVICIOS (
    gbic_op_name      string,
    id_dia_mes        string,
    msisdn            bigint,
    id_dia_alta       string,
    id_servicio       int,
    cod_servicio_sg3g int,
    descripcion       string
) PARTITIONED BY (
    gbic_op_id        int,
    month             string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_MOV_DESC_TERM (
    gbic_op_name          string,
    id_dia_mes            string,
    tipo_movimiento       string,
    clase_movimiento      string,
    id_clase_movimiento   int,
    id_tipo_movimiento    int,
    subtipo_movimiento    string,
    id_subtipo_movimiento int
) PARTITIONED BY (
    gbic_op_id            int,
    month                 string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_MOV_TERM (
    gbic_op_name           string,
    id_dia_mes             string,
    msisdn                 bigint,
    id_dia_alta            string,
    fecha_movimiento       string,
    id_clase_movimiento    int,
    id_tipo_movimiento     int,
    id_subtipo_movimiento  int,
    dim_clase_movimiento   string,
    dim_tipo_movimiento    string,
    dim_subtipo_movimiento string
) PARTITIONED BY (
    gbic_op_id             int,
    month                  string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_MOVIMIENTOS_SERVICIOS (
    gbic_op_name     string,
    id_dia_mes       string,
    msisdn           bigint,
    id_dia_alta      string,
    id_cliente       bigint,
    fecha_movimiento string,
    id_servicio      int,
    descripcion      string,
    clase_movimiento string
) PARTITIONED BY (
    gbic_op_id       int,
    month            string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_POBLACIONES(
    gbic_op_name             string,
    id_dia_mes               string,
    id_poblacion             int,
    poblacion                string,
    comunidad_autonoma       string,
    id_comunidad_autonoma    int,
    id_direccion_territorial int,
    direccion_territorial    string,
    provincia                string,
    id_provincia             int
) PARTITIONED BY (
    gbic_op_id               int,
    month                    string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_SEGMENTO_ORGANIZATIVO(
    gbic_op_name                  string,
    id_dia_mes                    string,
    id_identificador_segmentacion int,
    id_segmento_organizativo      int,
    segmento_organizativo         string,
    id_subsegmento_organizativo   int,
    subsegmento_organizativo      string,
    id_tramo_edad                 int,
    tramo_edad                    string,
    id_procedencia                int,
    procedencia                   string,
    id_fiabilidad                 int,
    fiabilidad                    string,
    id_provincia                  int
) PARTITIONED BY (
    gbic_op_id                    int,
    month                         string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_SEGMENTOS_TERM(
    gbic_op_name            string,
    id_dia_mes              string,
    id_tipo_cliente_negocio int,
    des_segmento_valor      string,
    id_segmento             int,
    des_segmento_negocio    string
) PARTITIONED BY (
    gbic_op_id              int,
    month                   string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_TERM_VOZDATOS1(
    gbic_op_name string,
    id_dia_mes   string,
    msisdn       bigint,
    imei         bigint,
    vol_black    double,
    vol_chat     double,
    vol_emocion  double,
    vol_internet double,
    vol_intranet double,
    vol_m_emp    double,
    vol_m_prof   double,
    vol_navega   double,
    vol_wifi     double,
    vol_resto    double
) PARTITIONED BY (
    gbic_op_id   int,
    month        string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_TERM_VOZDATOS2(
    gbic_op_name        string,
    id_dia_mes          string,
    msisdn              bigint,
    imei                bigint,
    id_dia_alta         string,
    min_voz_entrante_3g double,
    min_voz_saliente_3g double,
    min_voz_entrante_2g double,
    min_voz_saliente_2g double,
    num_llam_voz_ent_2g double,
    num_llam_voz_sal_2g double,
    num_llam_voz_ent_3g double,
    num_llam_voz_sal_3g double
) PARTITIONED BY (
    gbic_op_id          int,
    month               string)
STORED AS ORC;

CREATE TABLE GBIC_GLOBAL_ESP_TRAF_VOZ_HRC(
    gbic_op_name         string,
    id_dia_mes           string,
    msisdn               bigint,
    imei                 bigint,
    segundos_ent_dom_mad int,
    segundos_ent_dom_man int,
    segundos_ent_dom_tar int,
    segundos_ent_dom_noc int,
    segundos_ent_lun_mad int,
    segundos_ent_lun_man int,
    segundos_ent_lun_tar int,
    segundos_ent_lun_noc int,
    segundos_ent_mar_mad int,
    segundos_ent_mar_man int,
    segundos_ent_mar_tar int,
    segundos_ent_mar_noc int,
    segundos_ent_mie_mad int,
    segundos_ent_mie_man int,
    segundos_ent_mie_tar int,
    segundos_ent_mie_noc int,
    segundos_ent_jue_mad int,
    segundos_ent_jue_man int,
    segundos_ent_jue_tar int,
    segundos_ent_jue_noc int,
    segundos_ent_vie_mad int,
    segundos_ent_vie_man int,
    segundos_ent_vie_tar int,
    segundos_ent_vie_noc int,
    segundos_ent_sab_mad int,
    segundos_ent_sab_man int,
    segundos_ent_sab_tar int,
    segundos_ent_sab_noc int,
    segundos_sal_dom_mad int,
    segundos_sal_dom_man int,
    segundos_sal_dom_tar int,
    segundos_sal_dom_noc int,
    segundos_sal_lun_mad int,
    segundos_sal_lun_man int,
    segundos_sal_lun_tar int,
    segundos_sal_lun_noc int,
    segundos_sal_mar_mad int,
    segundos_sal_mar_man int,
    segundos_sal_mar_tar int,
    segundos_sal_mar_noc int,
    segundos_sal_mie_mad int,
    segundos_sal_mie_man int,
    segundos_sal_mie_tar int,
    segundos_sal_mie_noc int,
    segundos_sal_jue_mad int,
    segundos_sal_jue_man int,
    segundos_sal_jue_tar int,
    segundos_sal_jue_noc int,
    segundos_sal_vie_mad int,
    segundos_sal_vie_man int,
    segundos_sal_vie_tar int,
    segundos_sal_vie_noc int,
    segundos_sal_sab_mad int,
    segundos_sal_sab_man int,
    segundos_sal_sab_tar int,
    segundos_sal_sab_noc int,
    llamadas_ent_dom_mad int,
    llamadas_ent_dom_man int,
    llamadas_ent_dom_tar int,
    llamadas_ent_dom_noc int,
    llamadas_ent_lun_mad int,
    llamadas_ent_lun_man int,
    llamadas_ent_lun_tar int,
    llamadas_ent_lun_noc int,
    llamadas_ent_mar_mad int,
    llamadas_ent_mar_man int,
    llamadas_ent_mar_tar int,
    llamadas_ent_mar_noc int,
    llamadas_ent_mie_mad int,
    llamadas_ent_mie_man int,
    llamadas_ent_mie_tar int,
    llamadas_ent_mie_noc int,
    llamadas_ent_jue_mad int,
    llamadas_ent_jue_man int,
    llamadas_ent_jue_tar int,
    llamadas_ent_jue_noc int,
    llamadas_ent_vie_mad int,
    llamadas_ent_vie_man int,
    llamadas_ent_vie_tar int,
    llamadas_ent_vie_noc int,
    llamadas_ent_sab_mad int,
    llamadas_ent_sab_man int,
    llamadas_ent_sab_tar int,
    llamadas_ent_sab_noc int,
    llamadas_sal_dom_mad int,
    llamadas_sal_dom_man int,
    llamadas_sal_dom_tar int,
    llamadas_sal_dom_noc int,
    llamadas_sal_lun_mad int,
    llamadas_sal_lun_man int,
    llamadas_sal_lun_tar int,
    llamadas_sal_lun_noc int,
    llamadas_sal_mar_mad int,
    llamadas_sal_mar_man int,
    llamadas_sal_mar_tar int,
    llamadas_sal_mar_noc int,
    llamadas_sal_mie_mad int,
    llamadas_sal_mie_man int,
    llamadas_sal_mie_tar int,
    llamadas_sal_mie_noc int,
    llamadas_sal_jue_mad int,
    llamadas_sal_jue_man int,
    llamadas_sal_jue_tar int,
    llamadas_sal_jue_noc int,
    llamadas_sal_vie_mad int,
    llamadas_sal_vie_man int,
    llamadas_sal_vie_tar int,
    llamadas_sal_vie_noc int,
    llamadas_sal_sab_mad int,
    llamadas_sal_sab_man int,
    llamadas_sal_sab_tar int,
    llamadas_sal_sab_noc int
) PARTITIONED BY (
    gbic_op_id           int,
    month                string)
STORED AS ORC;


-- **************************************
-- TABLES WITH AGGREGATED DATA: ESP
-- **************************************
