/* lineas_miltisim.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/LINEAS_MULTISIM/month=$nominalTime/LINEAS_MULTISIM-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:          chararray,
        msisdn:              long,
        id_cliente:          chararray,
        id_dia_alta:         chararray,
        extension:           long,
        max_id_dia_alta_ext: chararray,
        min_id_dia_alta_ext: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                  AS (gbic_op_name:        chararray),
    mes                                AS (id_dia_mes:          chararray),
    msisdn                             AS (msisdn:              long),
    (long)REPLACE(id_cliente,'\\.','') AS (id_cliente:          long),
    REPLACE(id_dia_alta,'\\.','')      AS (id_dia_alta:         chararray),
    extension                          AS (extension:           long),
    max_id_dia_alta_ext                AS (max_id_dia_alta_ext: chararray),
    min_id_dia_alta_ext                AS (min_id_dia_alta_ext: chararray),
    1                                  AS (gbic_op_id:          int),
    '$nominalTime'                     AS (month:               chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_LINEAS_MULTISIM'
    USING org.apache.hcatalog.pig.HCatStorer();
