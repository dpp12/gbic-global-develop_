/* canjes.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/CANJES/month=2015-05-01/CANJES-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (mes:                     chararray,
        msisdn:                  chararray,
        id_cliente:              chararray,
        id_dia_alta:             chararray,
        fecha_ejecucion:         chararray,
        imei:                    long,
        tipo_canje_estrena:      chararray,
        descripcion:             chararray,
        importe_comision:        double,
        imp_terminal:            double,
        puntos_canjeados:        double,
        importe_adicional_canje: double,
        puntos_tras_canje:       double,
        valor_punto:             double
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY mes!='MES';

store_data = FOREACH noheader_data {
     id_dia_mes = CONCAT(CONCAT(CONCAT(SUBSTRING(mes, 0, 4), '-'), CONCAT(SUBSTRING(mes, 4, 6), '-')),SUBSTRING(mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                  AS (gbic_op_name:            chararray),
    id_dia_mes                         AS (id_dia_mes:              chararray),
    (long)REPLACE(msisdn,'\\.','')     AS (msisdn:                  long),
    (long)REPLACE(id_cliente,'\\.','') AS (id_cliente:              long),
    id_dia_alta                        AS (id_dia_alta:             chararray),
    fecha_ejecucion                    AS (fecha_ejecucion:         chararray),
    imei                               AS (imei:                    long),
    tipo_canje_estrena                 AS (tipo_canje_estrena:      chararray),
    descripcion                        AS (descripcion:             chararray),
    importe_comision                   AS (importe_comision:        double),
    imp_terminal                       AS (imp_terminal:            double),
    puntos_canjeados                   AS (puntos_canjeados:        double),
    importe_adicional_canje            AS (importe_adicional_canje: double),
    puntos_tras_canje                  AS (puntos_tras_canje:       double),
    valor_punto                        AS (valor_punto:             double),
    1                                  AS (gbic_op_id:              int),
    '2015-05-01'                       AS (month:                   chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_CANJES'
    USING org.apache.hcatalog.pig.HCatStorer();
