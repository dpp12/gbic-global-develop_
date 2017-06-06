/* segmentos_term.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/SEGMENTOS_TERM/month=$nominalTime/SEGMENTOS_TERM-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:              chararray,
        id_tipo_cliente_negocio: chararray,
        des_segmento_valor:      chararray,
        id_segmento:             chararray,
        des_segmento_negocio:    chararray	
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                              AS (gbic_op_name:            chararray),
    mes                                            AS (id_dia_mes:              chararray),
    (int)REPLACE(id_tipo_cliente_negocio,'\\.','') AS (id_tipo_cliente_negocio: int),
    des_segmento_valor                             AS (des_segmento_valor:      chararray),
    (int)REPLACE(id_segmento,'\\.','')             AS (id_segmento:             int),
    des_segmento_negocio                           AS (des_segmento_negocio:    chararray),
    1                                              AS (gbic_op_id:              int),
    '$nominalTime'                                 AS (month:                   chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_SEGMENTOS_TERM'
    USING org.apache.hcatalog.pig.HCatStorer();
