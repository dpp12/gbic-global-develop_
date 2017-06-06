/* inf_linea_valor_em.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/INF_LINEA_VALOR_EM/month=2015-05-01/INF_LINEA_VALOR_EM-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:         chararray,
        msisdn:             long,
        co_corporativo:     int,
        cl_segmento:        chararray,
        in_inactiva:        int,
        im_compromiso:      double,
        im_exencion:        double,
        im_cuotas_voz_nac:  double,
        im_cuotas_dat_nac:  double,
        im_cuotas_mix_nac:  double,
        im_cuotas_otr_nac:  double,
        im_cuotas_voz_roa:  double,
        im_cuotas_dat_roa:  double,
        im_cuotas_mix_roa:  double,
        im_cuotas_otr_roa:  double,
        im_cuotas_voz_res:  double,
        im_cuotas_dat_res:  double,
        im_cuotas_mix_res:  double,
        im_cuotas_otr_res:  double,
        im_trafico_voz_nac: double,
        im_trafico_dat_nac: double,
        im_trafico_otr_nac: double,
        im_trafico_voz_roa: double,
        im_trafico_dat_roa: double,
        im_trafico_otr_roa: double,
        im_trafico_voz_res: double,
        im_trafico_dat_res: double,
        im_trafico_otr_res: double,
        im_cuotas_nac:      double,
        im_cuotas_roa:      double,
        im_cuotas_res:      double,
        im_trafico_nac:     double,
        im_trafico_roa:     double,
        im_trafico_res:     double,
        im_varios_nac:      double,
        im_varios_roa:      double,
        im_varios_res:      double,
        im_resto_no_clasif: double,
        im_cuota_fusion:    double,
        im_otros_fusion:    double,
        to_linea:           double
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA'                  AS (gbic_op_name:       chararray),
    mes                                AS (id_dia_mes:         chararray),
    msisdn                             AS (msisdn:             long),
    co_corporativo                     AS (co_corporativo:     int),
    (int)REPLACE(cl_segmento,'\\.','') AS (cl_segmento:        int),
    in_inactiva                        AS (in_inactiva:        int),
    im_compromiso                      AS (im_compromiso:      double),
    im_exencion                        AS (im_exencion:        double),
    im_cuotas_voz_nac                  AS (im_cuotas_voz_nac:  double),
    im_cuotas_dat_nac                  AS (im_cuotas_dat_nac:  double),
    im_cuotas_mix_nac                  AS (im_cuotas_mix_nac:  double),
    im_cuotas_otr_nac                  AS (im_cuotas_otr_nac:  double),
    im_cuotas_voz_roa                  AS (im_cuotas_voz_roa:  double),
    im_cuotas_dat_roa                  AS (im_cuotas_dat_roa:  double),
    im_cuotas_mix_roa                  AS (im_cuotas_mix_roa:  double),
    im_cuotas_otr_roa                  AS (im_cuotas_otr_roa:  double),
    im_cuotas_voz_res                  AS (im_cuotas_voz_res:  double),
    im_cuotas_dat_res                  AS (im_cuotas_dat_res:  double),
    im_cuotas_mix_res                  AS (im_cuotas_mix_res:  double),
    im_cuotas_otr_res                  AS (im_cuotas_otr_res:  double),
    im_trafico_voz_nac                 AS (im_trafico_voz_nac: double),
    im_trafico_dat_nac                 AS (im_trafico_dat_nac: double),
    im_trafico_otr_nac                 AS (im_trafico_otr_nac: double),
    im_trafico_voz_roa                 AS (im_trafico_voz_roa: double),
    im_trafico_dat_roa                 AS (im_trafico_dat_roa: double),
    im_trafico_otr_roa                 AS (im_trafico_otr_roa: double),
    im_trafico_voz_res                 AS (im_trafico_voz_res: double),
    im_trafico_dat_res                 AS (im_trafico_dat_res: double),
    im_trafico_otr_res                 AS (im_trafico_otr_res: double),
    im_cuotas_nac                      AS (im_cuotas_nac:      double),
    im_cuotas_roa                      AS (im_cuotas_roa:      double),
    im_cuotas_res                      AS (im_cuotas_res:      double),
    im_trafico_nac                     AS (im_trafico_nac:     double),
    im_trafico_roa                     AS (im_trafico_roa:     double),
    im_trafico_res                     AS (im_trafico_res:     double),
    im_varios_nac                      AS (im_varios_nac:      double),
    im_varios_roa                      AS (im_varios_roa:      double),
    im_varios_res                      AS (im_varios_res:      double),
    im_resto_no_clasif                 AS (im_resto_no_clasif: double),
    im_cuota_fusion                    AS (im_cuota_fusion:    double),
    im_otros_fusion                    AS (im_otros_fusion:    double),
    to_linea                           AS (to_linea:           double),
    1                                  AS (gbic_op_id:         int),
    '2015-05-01'                       AS (month:              chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_INF_LINEA_VALOR_EM'
    USING org.apache.hcatalog.pig.HCatStorer();
