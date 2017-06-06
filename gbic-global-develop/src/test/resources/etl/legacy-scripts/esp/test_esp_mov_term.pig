/* mov_term.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/MOV_TERM/month=2015-05-01/MOV_TERM-2015-05-01.csv'
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

dim_mov_desc_term = LOAD '/user/gplatform/inbox/esp/MOV_DESC_TERM/month=2015-05-01/MOV_DESC_TERM-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_id_dia_mes:            chararray,
        dim_tipo_movimiento:       chararray,
        dim_clase_movimiento:      chararray,
        dim_id_clase_movimiento:   int,
        dim_id_tipo_movimiento:    int,
        dim_subtipo_movimiento:    chararray,
        dim_id_subtipo_movimiento: int
        );

unique_data_dim_mov_desc_term   = DISTINCT dim_mov_desc_term;
noheader_data_dim_mov_desc_term = FILTER unique_data_dim_mov_desc_term BY dim_id_dia_mes!='ID_DIA_MES';

gbic_global_data = JOIN 
    noheader_data                   BY (id_clase_movimiento, id_tipo_movimiento, id_subtipo_movimiento) LEFT OUTER, 
    noheader_data_dim_mov_desc_term BY (dim_id_clase_movimiento, dim_id_tipo_movimiento, dim_id_subtipo_movimiento);

store_data = FOREACH gbic_global_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                  AS (gbic_op_name:           chararray),
    mes                                AS (id_dia_mes:             chararray),
    msisdn                             AS (msisdn:                 long),
    REPLACE(id_dia_alta,'\\.','')      AS (id_dia_alta:            chararray),
    REPLACE(fecha_movimiento,'\\.','') AS (fecha_movimiento:       chararray),
    id_clase_movimiento                AS (id_clase_movimiento:    int),
    id_tipo_movimiento                 AS (id_tipo_movimiento:     int),
    id_subtipo_movimiento              AS (id_subtipo_movimiento:  int),
    dim_clase_movimiento               AS (dim_clase_movimiento:   chararray),
    dim_tipo_movimiento                AS (dim_tipo_movimiento:    chararray),
    dim_subtipo_movimiento             AS (dim_subtipo_movimiento: chararray),
    1                                  AS (gbic_op_id:             int),
    '2015-05-01'                       AS (month:                  chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_MOV_TERM'
    USING org.apache.hcatalog.pig.HCatStorer();
