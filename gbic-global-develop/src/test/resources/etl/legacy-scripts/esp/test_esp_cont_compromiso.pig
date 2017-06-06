/* cont_compromiso.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/CONT_COMPROMISO/month=2015-05-01/CONT_COMPROMISO-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:           chararray,
        msisdn:               long,
        origen_contrato_cco:  chararray,
        des_causa_alta:       chararray,
        id_dia_firma:         chararray,
        imei:                 long,
        apoyo_economico:      int,
        importe_penalizacion: chararray,
        meses_permanencia:    int
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'              AS (gbic_op_name:         chararray),
    mes                            AS (id_dia_mes:           chararray),
    msisdn                         AS (msisdn:               long),
    origen_contrato_cco            AS (origen_contrato_cco:  chararray),
    des_causa_alta                 AS (des_causa_alta:       chararray),
    REPLACE(id_dia_firma,'\\.','') AS (id_dia_firma:         chararray),
    imei                           AS (imei:                 long),
    apoyo_economico                AS (apoyo_economico:      int),
    importe_penalizacion           AS (importe_penalizacion: chararray),
    meses_permanencia              AS (meses_permanencia:    int),	
    1                              AS (gbic_op_id:           int),
    '2015-05-01'                   AS (month:                chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_CONT_COMPROMISO'
    USING org.apache.hcatalog.pig.HCatStorer();
