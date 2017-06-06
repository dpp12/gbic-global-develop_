/* mov_term.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/MOV_TERM/month=$nominalTime/MOV_TERM-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:            chararray,
        msisdn:                long,
        id_dia_alta:           chararray,
        fecha_movimiento:      chararray,
        id_clase_movimiento:   int,
        id_tipo_movimiento:    int,
        id_subtipo_movimiento: int	
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                  AS (gbic_op_name:          chararray),
    mes                                AS (id_dia_mes:            chararray),
    msisdn                             AS (msisdn:                long),
    REPLACE(id_dia_alta,'\\.','')      AS (id_dia_alta:           chararray),
    REPLACE(fecha_movimiento,'\\.','') AS (fecha_movimiento:      chararray),
    id_clase_movimiento                AS (id_clase_movimiento:   int),
    id_tipo_movimiento                 AS (id_tipo_movimiento:    int),
    id_subtipo_movimiento              AS (id_subtipo_movimiento: int),
    1                                  AS (gbic_op_id:            int),
    '$nominalTime'                     AS (month:                 chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_MOV_TERM'
    USING org.apache.hcatalog.pig.HCatStorer();
