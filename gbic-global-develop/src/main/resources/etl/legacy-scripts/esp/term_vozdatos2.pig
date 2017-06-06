/* term_vozdatos2.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/TERM_VOZDATOS2/month=$nominalTime/TERM_VOZDATOS2-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:          chararray,
        msisdn:              long,
        imei:                long,
        id_dia_alta:         chararray,
        min_voz_entrante_3g: double,
        min_voz_saliente_3g: double,
        min_voz_entrante_2g: double,
        min_voz_saliente_2g: double,
        num_llam_voz_ent_2g: double,
        num_llam_voz_sal_2g: double,
        num_llam_voz_ent_3g: double,
        num_llam_voz_sal_3g: double
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'   AS (gbic_op_name:        chararray),
    mes                 AS (id_dia_mes:          chararray),
    msisdn              AS (msisdn:              long),
    imei                AS (imei:                long),
    id_dia_alta         AS (id_dia_alta:         chararray),
    min_voz_entrante_3g AS (min_voz_entrante_3g: double),
    min_voz_saliente_3g AS (min_voz_saliente_3g: double),
    min_voz_entrante_2g AS (min_voz_entrante_2g: double),
    min_voz_saliente_2g AS (min_voz_saliente_2g: double),
    num_llam_voz_ent_2g AS (num_llam_voz_ent_2g: double),
    num_llam_voz_sal_2g AS (num_llam_voz_sal_2g: double),
    num_llam_voz_ent_3g AS (num_llam_voz_ent_3g: double),
    num_llam_voz_sal_3g AS (num_llam_voz_sal_3g: double),
    1                   AS (gbic_op_id:          int),
    '$nominalTime'      AS (month:               chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_TERM_VOZDATOS2'
    USING org.apache.hcatalog.pig.HCatStorer();
