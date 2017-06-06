/* mov_desc_term.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/MOV_DESC_TERM/month=2015-05-01/MOV_DESC_TERM-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:            chararray,
        tipo_movimiento:       chararray,
        clase_movimiento:      chararray,
        id_clase_movimiento:   int,
        id_tipo_movimiento:    int,
        subtipo_movimiento:    chararray,
        id_subtipo_movimiento: int
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'     AS (gbic_op_name:          chararray),
    mes                   AS (id_dia_mes:            chararray),
    tipo_movimiento       AS (tipo_movimiento:       chararray),
    clase_movimiento      AS (clase_movimiento:      chararray),
    id_clase_movimiento   AS (id_clase_movimiento:   int),
    id_tipo_movimiento    AS (id_tipo_movimiento:    int),
    subtipo_movimiento    AS (subtipo_movimiento:    chararray),
    id_subtipo_movimiento AS (id_subtipo_movimiento: int),
    1                     AS (gbic_op_id:            int),
    '2015-05-01'          AS (month:                 chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_MOV_DESC_TERM'
    USING org.apache.hcatalog.pig.HCatStorer();
