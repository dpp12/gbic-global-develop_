/* movimientos_servicios.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/MOVIMIENTOS_SERVICIOS/month=$nominalTime/MOVIMIENTOS_SERVICIOS-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:       chararray,
        msisdn:           long,
        id_dia_alta:      chararray,
        id_cliente:       chararray,
        fecha_movimiento: chararray,
        id_servicio:      chararray,
        descripcion:      chararray,
        clase_movimiento: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_cliente!='ID_CLIENTE';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                   AS (gbic_op_name:     chararray),
    mes                                 AS (id_dia_mes:       chararray),
    msisdn                              AS (msisdn:           long),
    REPLACE(id_dia_alta,'\\.','')       AS (id_dia_alta:      chararray),
    (long)REPLACE(id_cliente,'\\.','')  AS (id_cliente:       long),
    REPLACE(fecha_movimiento,'\\.','')  AS (fecha_movimiento: chararray),
    (int)REPLACE(id_servicio,'\\.','')  AS (id_servicio:      int),
    descripcion                         AS (descripcion:      chararray),
    clase_movimiento                    AS (clase_movimiento: chararray),
    1                                   AS (gbic_op_id:       int),
    '$nominalTime'                      AS (month:            chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_MOVIMIENTOS_SERVICIOS'
    USING org.apache.hcatalog.pig.HCatStorer();
