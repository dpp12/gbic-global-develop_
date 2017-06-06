/* inf_linea_valor.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/INF_LINEA_VALOR/month=2015-05-01/INF_LINEA_VALOR-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:           chararray,
        msisdn:               long,
        co_cliente:           chararray,
        fx_dia_alta:          chararray,
        in_con_pre:           chararray,
        im_resto_no_clasif:   double,
        im_cons_min:          double,
        im_cons_min_dat:      double,
        im_compromiso:        double,
        im_voz:               double,
        im_voz_r:             double,
        im_datos:             double,
        im_datos_r:           double,
        im_eventos:           double,
        im_mms:               double,
        im_mms_r:             double,
        im_sms_mms:           double,
        im_sms:               double,
        im_sms_r:             double,
        im_recarga:           double,
        im_resto:             double,
        im_resto_r:           double,
        im_trafico_mixto:     double,
        im_trafico_mixto_r:   double,
        im_fusion_voz:        double,
        im_fusion_datos:      double,
        im_ing_itx_resto_voz: double,
        im_gas_itx_resto_voz: double,
        im_ing_itx_tesa_voz:  double,
        im_gas_itx_tesa_voz:  double,
        im_ing_itx_resto_sms: double,
        im_gas_itx_resto_sms: double,
        im_ing_itx_tesa_sms:  double,
        im_gas_itx_tesa_sms:  double,
        to_linea:             double
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                  AS (gbic_op_name:         chararray),
    mes                                AS (id_dia_mes:           chararray),
    msisdn                             AS (msisdn:               long),
    (long)REPLACE(co_cliente,'\\.','') AS (co_cliente:           long),
    REPLACE(fx_dia_alta,'\\/','-')     AS (fx_dia_alta:          chararray),
    in_con_pre                         AS (in_con_pre:           chararray),
    im_resto_no_clasif                 AS (im_resto_no_clasif:   double),
    im_cons_min                        AS (im_cons_min:          double),
    im_cons_min_dat                    AS (im_cons_min_dat:      double),
    im_compromiso                      AS (im_compromiso:        double),
    im_voz                             AS (im_voz:               double),
    im_voz_r                           AS (im_voz_r:             double),
    im_datos                           AS (im_datos:             double),
    im_datos_r                         AS (im_datos_r:           double),
    im_eventos                         AS (im_eventos:           double),
    im_mms                             AS (im_mms:               double),
    im_mms_r                           AS (im_mms_r:             double),
    im_sms_mms                         AS (im_sms_mms:           double),
    im_sms                             AS (im_sms:               double),
    im_sms_r                           AS (im_sms_r:             double),
    im_recarga                         AS (im_recarga:           double),
    im_resto                           AS (im_resto:             double),
    im_resto_r                         AS (im_resto_r:           double),
    im_trafico_mixto                   AS (im_trafico_mixto:     double),
    im_trafico_mixto_r                 AS (im_trafico_mixto_r:   double),
    im_fusion_voz                      AS (im_fusion_voz:        double),
    im_fusion_datos                    AS (im_fusion_datos:      double),
    im_ing_itx_resto_voz               AS (im_ing_itx_resto_voz: double),
    im_gas_itx_resto_voz               AS (im_gas_itx_resto_voz: double),
    im_ing_itx_tesa_voz                AS (im_ing_itx_tesa_voz:  double),
    im_gas_itx_tesa_voz                AS (im_gas_itx_tesa_voz:  double),
    im_ing_itx_resto_sms               AS (im_ing_itx_resto_sms: double),
    im_gas_itx_resto_sms               AS (im_gas_itx_resto_sms: double),
    im_ing_itx_tesa_sms                AS (im_ing_itx_tesa_sms:  double),
    im_gas_itx_tesa_sms                AS (im_gas_itx_tesa_sms:  double),
    to_linea                           AS (to_linea:             double),
    1                                  AS (gbic_op_id:           int),
    '2015-05-01'                       AS (month:                chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_INF_LINEA_VALOR'
    USING org.apache.hcatalog.pig.HCatStorer();
