/* poblaciones.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/POBLACIONES/month=2015-05-01/POBLACIONES-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:               chararray,
        id_poblacion:             int,
        poblacion:                chararray,
        comunidad_autonoma:       chararray,
        id_comunidad_autonoma:    int,
        id_direccion_territorial: int,
        direccion_territorial:    chararray,
        provincia:                chararray,
        id_provincia:             int
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'        AS (gbic_op_name:             chararray),
    mes                      AS (id_dia_mes:               chararray),
    id_poblacion             AS (id_poblacion:             int),
    poblacion                AS (poblacion:                chararray),
    comunidad_autonoma       AS (comunidad_autonoma:       chararray),
    id_comunidad_autonoma    AS (id_comunidad_autonoma:    int),
    id_direccion_territorial AS (id_direccion_territorial: int),
    direccion_territorial    AS (direccion_territorial:    chararray),
    provincia                AS (provincia:                chararray),
    id_provincia             AS (id_provincia:             int),
    1                        AS (gbic_op_id:               int),
    '2015-05-01'             AS (month:                    chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_POBLACIONES'
    USING org.apache.hcatalog.pig.HCatStorer();
