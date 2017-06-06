/* segmento_organizativo.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/SEGMENTO_ORGANIZATIVO/month=$nominalTime/SEGMENTO_ORGANIZATIVO-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:                    chararray,
        id_identificador_segmentacion: int,
        id_segmento_organizativo:      int,
        segmento_organizativo:         chararray,
        id_subsegmento_organizativo:   int,
        subsegmento_organizativo:      chararray,
        id_tramo_edad:                 int,
        tramo_edad:                    chararray,
        id_procedencia:                int,
        procedencia:                   chararray,
        id_fiabilidad:                 int,
        fiabilidad:                    chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'             AS (gbic_op_name:                  chararray),
    mes                           AS (id_dia_mes:                    chararray),
    id_identificador_segmentacion AS (id_identificador_segmentacion: int),
    id_segmento_organizativo      AS (id_segmento_organizativo:      int),
    segmento_organizativo         AS (segmento_organizativo:         chararray),
    id_subsegmento_organizativo   AS (id_subsegmento_organizativo:   int),
    subsegmento_organizativo      AS (subsegmento_organizativo:      chararray),
    id_tramo_edad                 AS (id_tramo_edad:                 int),
    tramo_edad                    AS (tramo_edad:                    chararray),
    id_procedencia                AS (id_procedencia:                int),
    procedencia                   AS (procedencia:                   chararray),
    id_fiabilidad                 AS (id_fiabilidad:                 int),
    fiabilidad                    AS (fiabilidad:                    chararray),
    1                             AS (gbic_op_id:                    int),
    '$nominalTime'                AS (month:                         chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_SEGMENTO_ORGANIZATIVO'
    USING org.apache.hcatalog.pig.HCatStorer();
