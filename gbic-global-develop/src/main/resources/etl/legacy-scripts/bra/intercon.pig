/* intercon.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/INTERCON/month=$nominalTime/INTERCON-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:                 chararray,
        msisdn:                   long,
        cd_tipo_cobranca:         int,
        cd_sentido_chamada:       int,
        cd_sentido_cobranca:      int,
        cd_tipo_trafego:          int,
        cd_direcao_chamada:       int,
        cd_situ_chamado_chamador: int,
        qt_seg_real:              int,
        qt_seg_tarifado:          int,
        qt_chamada_intercon:      int,
        vl_bruto_intercon:        chararray,
        vl_tarifa_intercon:       chararray,
        vl_liquido_intercon:      chararray,
        vl_imposto_intercon:      chararray
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'                                  AS (gbic_op_name:             chararray),
    CONCAT(month_id, '-01')                        AS (id_month:                 chararray),
    msisdn                                         AS (msisdn:                   long),
    cd_tipo_cobranca                               AS (cd_tipo_cobranca:         int),
    cd_sentido_chamada                             AS (cd_sentido_chamada:       int),
    cd_sentido_cobranca                            AS (cd_sentido_cobranca:      int),
    cd_tipo_trafego                                AS (cd_tipo_trafego:          int),
    cd_direcao_chamada                             AS (cd_direcao_chamada:       int),
    cd_situ_chamado_chamador                       AS (cd_situ_chamado_chamador: int),
    qt_seg_real                                    AS (qt_seg_real:              int),
    qt_seg_tarifado                                AS (qt_seg_tarifado:          int),
    qt_chamada_intercon                            AS (qt_chamada_intercon:      int),
    (double)REPLACE(vl_bruto_intercon,'\\,','.')   AS (vl_bruto_intercon:        double),
    (double)REPLACE(vl_tarifa_intercon,'\\,','.')  AS (vl_tarifa_intercon:       double),
    (double)REPLACE(vl_liquido_intercon,'\\,','.') AS (vl_liquido_intercon:      double),
    (double)REPLACE(vl_imposto_intercon,'\\,','.') AS (vl_imposto_intercon:      double),
    201                                            AS (gbic_op_id:               int),
    '$nominalTime'                                 AS (month:                    chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_INTERCON'
    USING org.apache.hcatalog.pig.HCatStorer();
