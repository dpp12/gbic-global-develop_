/* imeishist_B.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/B_IMEISHIST/month=2015-05-01/B_IMEISHIST-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:   chararray,
        msisdn:       long,
        cod_almacen:  chararray,
        nom_almacen:  chararray,
        cod_producto: int,
        des_producto: chararray,
        cod_swit:     int,
        fecha_alta:   chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                              AS (gbic_op_name: chararray),
    mes                                            AS (id_dia_mes:   chararray),
    msisdn                                         AS (msisdn:       long),
    cod_almacen                                    AS (cod_almacen:  chararray),
    nom_almacen                                    AS (nom_almacen:  chararray),
    cod_producto                                   AS (cod_producto: int),
    des_producto                                   AS (des_producto: chararray),
    cod_swit                                       AS (cod_swit:     int),
    REPLACE(fecha_alta,'\\/','-')                  AS (fecha_alta:   chararray),
    1                                              AS (gbic_op_id:   int),
    '2015-05-01'                                   AS (month:        chararray),
    'period_B'                                     AS (fortnight:    chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_IMEISHIST'
    USING org.apache.hcatalog.pig.HCatStorer();
