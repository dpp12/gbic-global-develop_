/* inf_lin_term1.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/INF_LIN_TERM1/month=$nominalTime/INF_LIN_TERM1-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:                     chararray,
        id_cliente:                     chararray,
        msisdn:                         long,
        id_dia_alta:                    chararray,
        cli_fecha_baja:                 chararray,
        id_combinacion_contrato:        chararray,
        fecha_nacimiento:               chararray,
        sexo:                           chararray,
        id_identificador_segmentacion:  chararray,
        id_tipo_cliente_negocio:        chararray,
        id_poblacion:                   chararray,
        id_estado_linea:                chararray,
        ind_pque_activo:                chararray,
        ind_tipo_linea:                 chararray,
        imei:                           long,
        importe_total_salida_mc:        chararray,
        llamadas_salida_rc:             chararray,
        llamadas_total_salida_mc:       chararray,
        importe_total_entrada_mc:       chararray,
        llamadas_total_entrada_mc:      chararray,
        llamadas_crc:                   chararray,
        llamadas_prog_puntos:           chararray,
        llamadas_entrada_rgtr:          chararray,
        llamadas_entrada_rc:            chararray,
        seg_aire_entrada_rgtr:          chararray,
        seg_aire_salida_rgtr_total:     chararray,
        seg_aire_salida_rgtr_rc:        chararray,
        seg_fact_salida_rc:             chararray,
        llamadas_salida_rgtr_total:     chararray,
        llamadas_entrada_inter:         chararray,
        llamadas_salida_inter:          chararray,
        seg_aire_entrada_inter:         chararray,
        seg_aire_salida_inter:          chararray,
        llamadas_entrada_fijo_tele:     chararray,
        llamadas_salida_fijo_tele:      chararray,
        seg_aire_entrada_fijo_tele:     chararray,
        seg_aire_salida_fijo_tele:      chararray,
        llamadas_entrada_fijo_otros:    chararray,
        llamadas_salida_fijo_otros:     chararray,
        seg_aire_entrada_fijo_otros:    chararray,
        seg_aire_salida_fijo_otros:     chararray,
        llamadas_entrada_otros_mov:     chararray,
        llamadas_salida_otros_mov:      chararray,
        seg_aire_entrada_otros_mov:     chararray,
        seg_aire_salida_otros_mov:      chararray,
        num_rec_coste_mes:              chararray,
        num_rec_prom_mes:               chararray,
        imp_rec_coste_mes:              double,
        imp_rec_prom_mes:               double,
        imp_saldo_mes:                  double,
        vol_down_gprs:                  chararray,
        vol_downlink_gprs_rc:           chararray,
        vol_uplink_gprs:                chararray,
        vol_uplink_gprs_rc:             double,
        neto_facturacion:               double,
        importe_actuaciones:            double,
        mms_entrada:                    chararray,
        mms_salida:                     chararray,
        imp_mms_entrada:                double,
        imp_mms_salida:                 double,
        eventos_premium:                chararray,
        importe_premium:                double
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                                    AS (gbic_op_name:                  chararray),
    mes                                                  AS (id_dia_mes:                    chararray),
    (long)REPLACE(id_cliente,'\\.','')                   AS (id_cliente:                    long),
    msisdn                                               AS (msisdn:                        long),
    id_dia_alta                                          AS (id_dia_alta:                   chararray),
    cli_fecha_baja                                       AS (cli_fecha_baja:                chararray),
    (long)REPLACE(id_combinacion_contrato,'\\.','')      AS (id_combinacion_contrato:       long),
    fecha_nacimiento                                     AS (fecha_nacimiento:              chararray),
    sexo                                                 AS (sexo:                          chararray),
    (int)REPLACE(id_identificador_segmentacion,'\\.','') AS (id_identificador_segmentacion: int),
    (int)REPLACE(id_tipo_cliente_negocio,'\\.','')       AS (id_tipo_cliente_negocio:       int),
    (int)REPLACE(id_poblacion,'\\.','')                  AS (id_poblacion:                  int),
    (int)REPLACE(id_estado_linea,'\\.','')               AS (id_estado_linea:               int),
    ind_pque_activo                                      AS (ind_pque_activo:               chararray),
    ind_tipo_linea                                       AS (ind_tipo_linea:                chararray),
    imei                                                 AS (imei:                          long),
    (int)REPLACE(importe_total_salida_mc,'\\.','')       AS (importe_total_salida_mc:       int),
    (int)REPLACE(llamadas_salida_rc,'\\.','')            AS (llamadas_salida_rc:            int),
    (int)REPLACE(llamadas_total_salida_mc,'\\.','')      AS (llamadas_total_salida_mc:      int),
    (int)REPLACE(importe_total_entrada_mc,'\\.','')      AS (importe_total_entrada_mc:      int),
    (int)REPLACE(llamadas_total_entrada_mc,'\\.','')     AS (llamadas_total_entrada_mc:     int),
    (int)REPLACE(llamadas_crc,'\\.','')                  AS (llamadas_crc:                  int),
    (int)REPLACE(llamadas_prog_puntos,'\\.','')          AS (llamadas_prog_puntos:          int),
    (int)REPLACE(llamadas_entrada_rgtr,'\\.','')         AS (llamadas_entrada_rgtr:         int),
    (int)REPLACE(llamadas_entrada_rc,'\\.','')           AS (llamadas_entrada_rc:           int),
    (int)REPLACE(seg_aire_entrada_rgtr,'\\.','')         AS (seg_aire_entrada_rgtr:         int),
    (int)REPLACE(seg_aire_salida_rgtr_total,'\\.','')    AS (seg_aire_salida_rgtr_total:    int),
    (int)REPLACE(seg_aire_salida_rgtr_rc,'\\.','')       AS (seg_aire_salida_rgtr_rc:       int),
    (int)REPLACE(seg_fact_salida_rc,'\\.','')            AS (seg_fact_salida_rc:            int),
    (int)REPLACE(llamadas_salida_rgtr_total,'\\.','')    AS (llamadas_salida_rgtr_total:    int),
    (int)REPLACE(llamadas_entrada_inter,'\\.','')        AS (llamadas_entrada_inter:        int),
    (int)REPLACE(llamadas_salida_inter,'\\.','')         AS (llamadas_salida_inter:         int),
    (int)REPLACE(seg_aire_entrada_inter,'\\.','')        AS (seg_aire_entrada_inter:        int),
    (int)REPLACE(seg_aire_salida_inter,'\\.','')         AS (seg_aire_salida_inter:         int),
    (int)REPLACE(llamadas_entrada_fijo_tele,'\\.','')    AS (llamadas_entrada_fijo_tele:    int),
    (int)REPLACE(llamadas_salida_fijo_tele,'\\.','')     AS (llamadas_salida_fijo_tele:     int),
    (int)REPLACE(seg_aire_entrada_fijo_tele,'\\.','')    AS (seg_aire_entrada_fijo_tele:    int),
    (int)REPLACE(seg_aire_salida_fijo_tele,'\\.','')     AS (seg_aire_salida_fijo_tele:     int),
    (int)REPLACE(llamadas_entrada_fijo_otros,'\\.','')   AS (llamadas_entrada_fijo_otros:   int),
    (int)REPLACE(llamadas_salida_fijo_otros,'\\.','')    AS (llamadas_salida_fijo_otros:    int),
    (int)REPLACE(seg_aire_entrada_fijo_otros,'\\.','')   AS (seg_aire_entrada_fijo_otros:   int),
    (int)REPLACE(seg_aire_salida_fijo_otros,'\\.','')    AS (seg_aire_salida_fijo_otros:    int),
    (int)REPLACE(llamadas_entrada_otros_mov,'\\.','')    AS (llamadas_entrada_otros_mov:    int),
    (int)REPLACE(llamadas_salida_otros_mov,'\\.','')     AS (llamadas_salida_otros_mov:     int),
    (int)REPLACE(seg_aire_entrada_otros_mov,'\\.','')    AS (seg_aire_entrada_otros_mov:    int),
    (int)REPLACE(seg_aire_salida_otros_mov,'\\.','')     AS (seg_aire_salida_otros_mov:     int),
    (int)REPLACE(num_rec_coste_mes,'\\.','')             AS (num_rec_coste_mes:             int),
    (int)REPLACE(num_rec_prom_mes,'\\.','')              AS (num_rec_prom_mes:              int),
    imp_rec_coste_mes                                    AS (imp_rec_coste_mes:             double),
    imp_rec_prom_mes                                     AS (imp_rec_prom_mes:              double),
    imp_saldo_mes                                        AS (imp_saldo_mes:                 double),
    (long)REPLACE(vol_down_gprs,'\\.','')                AS (vol_down_gprs:                 long),
    (long)REPLACE(vol_downlink_gprs_rc,'\\.','')         AS (vol_downlink_gprs_rc:          long),
    (long)REPLACE(vol_uplink_gprs,'\\.','')              AS (vol_uplink_gprs:               long),
    vol_uplink_gprs_rc                                   AS (vol_uplink_gprs_rc:            double),
    neto_facturacion                                     AS (neto_facturacion:              double),
    importe_actuaciones                                  AS (importe_actuaciones:           double),
    (long)REPLACE(mms_entrada,'\\.','')                  AS (mms_entrada:                   long),
    (long)REPLACE(mms_salida,'\\.','')                   AS (mms_salida:                    long),
    imp_mms_entrada                                      AS (imp_mms_entrada:               double),
    imp_mms_salida                                       AS (imp_mms_salida:                double),
    (long)REPLACE(eventos_premium,'\\.','')              AS (eventos_premium:               long),
    importe_premium                                      AS (importe_premium:               double),
    1                                                    AS (gbic_op_id:                    int),
    '$nominalTime'                                       AS (month:                         chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_INF_LIN_TERM1'
    USING org.apache.hcatalog.pig.HCatStorer();