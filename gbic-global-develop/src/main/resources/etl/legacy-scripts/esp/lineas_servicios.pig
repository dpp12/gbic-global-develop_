/* lineas_servicios.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/LINEAS_SERVICIOS/month=$nominalTime/LINEAS_SERVICIOS-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:        chararray,
        msisdn:            long,
        id_dia_alta:       chararray,
        id_servicio:       chararray,
        cod_servicio_sg3g: int,
        descripcion:       chararray	
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                   AS (gbic_op_name:      chararray),
    mes                                 AS (id_dia_mes:        chararray),
    msisdn                              AS (msisdn:            long),
    id_dia_alta                         AS (id_dia_alta:       chararray),
    (int)REPLACE(id_servicio,'\\.','')  AS (id_servicio:       int),
    cod_servicio_sg3g                   AS (cod_servicio_sg3g: int),
    descripcion                         AS (descripcion:       chararray),
    1                                   AS (gbic_op_id:        int),
    '$nominalTime'                      AS (month:             chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_LINEAS_SERVICIOS'
    USING org.apache.hcatalog.pig.HCatStorer();
