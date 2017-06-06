/* estados_lineas.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/ESTADOS_LINEAS/month=$nominalTime/ESTADOS_LINEAS-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:      chararray,
        id_estado_linea: chararray,
        estado_linea:    chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

 store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                      AS (gbic_op_name:    chararray),
    mes                                    AS (id_dia_mes:      chararray),
    (int)REPLACE(id_estado_linea,'\\.','') AS (id_estado_linea: int),
    estado_linea                           AS (estado_linea:    chararray),
    1                                      AS (gbic_op_id:      int),
    '$nominalTime'                         AS (month:           chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_ESTADOS_LINEAS'
    USING org.apache.hcatalog.pig.HCatStorer();
