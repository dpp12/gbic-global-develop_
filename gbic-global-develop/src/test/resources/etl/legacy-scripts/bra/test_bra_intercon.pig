/* intercon.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/INTERCON/month=2015-04-01/INTERCON-2015-04-01.csv'
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


dim_tipo_trafego = LOAD '/user/gplatform/inbox/bra/DIM_TIPO_TRAFEGO/month=2015-06-01/DIM_TIPO_TRAFEGO-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_cd_tipo_trafego: int,
        dim_ds_tipo_trafego: chararray
        );

unique_data_dim_tipo_trafego   = DISTINCT dim_tipo_trafego;
noheader_data_dim_tipo_trafego = FILTER unique_data_dim_tipo_trafego BY ds_tipo_trafego!='DS_TIPO_TRAFEGO';


dim_tipo_cobranca = LOAD '/user/gplatform/inbox/bra/DIM_TIPO_COBRANCA/month=2015-06-01/DIM_TIPO_COBRANCA-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_cd_tipo_cobranca: int,
        dim_ds_tipo_cobranca: chararray
        );

unique_data_dim_tipo_cobranca   = DISTINCT dim_tipo_cobranca;
noheader_data_dim_tipo_cobranca = FILTER unique_data_dim_tipo_cobranca BY ds_tipo_cobranca!='DS_TIPO_COBRANCA';

dim_chamado_chamador = LOAD '/user/gplatform/inbox/bra/DIM_SITU_CHAMADO_CHAMADOR/month=2015-06-01/DIM_SITU_CHAMADO_CHAMADOR-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_cd_situ_chamado_chamador: int,
        dim_ds_situ_chamado_chamador: chararray
        );

unique_data_dim_chamado_chamador   = DISTINCT dim_chamado_chamador;
noheader_data_dim_chamado_chamador = FILTER unique_data_dim_chamado_chamador BY ds_situ_chamado_chamador!='DS_SITU_CHAMADO_CHAMADOR';

dim_sentido_cobranca = LOAD '/user/gplatform/inbox/bra/DIM_SENTIDO_COBRANCA/month=2015-06-01/DIM_SENTIDO_COBRANCA-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_cd_sentido_cobranca: int,
        dim_ds_sentido_cobranca: chararray
        );

unique_data_dim_sentido_cobranca   = DISTINCT dim_sentido_cobranca;
noheader_data_dim_sentido_cobranca = FILTER unique_data_dim_sentido_cobranca BY ds_sentido_cobranca!='DS_SENTIDO_COBRANCA';


dim_sentido_chamada = LOAD '/user/gplatform/inbox/bra/DIM_SENTIDO_CHAMADA/month=2015-06-01/DIM_SENTIDO_CHAMADA-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_cd_sentido_chamada: int,
        dim_ds_sentido_chamada: chararray
        );

unique_data_dim_sentido_chamada   = DISTINCT dim_sentido_chamada;
noheader_data_dim_sentido_chamada = FILTER unique_data_dim_sentido_chamada BY ds_sentido_chamada!='DS_SENTIDO_CHAMADA';


dim_direcao_chamada = LOAD '/user/gplatform/inbox/bra/DIM_DIRECAO_CHAMADA/month=2015-06-01/DIM_DIRECAO_CHAMADA-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (dim_cd_direcao_chamada: int,
        dim_ds_direcao_chamada: chararray
        );

unique_data_dim_direcao_chamada   = DISTINCT dim_direcao_chamada;
noheader_data_dim_direcao_chamada = FILTER unique_data_dim_direcao_chamada BY ds_direcao_chamada!='DS_CIRECAO_CHAMADA';

gbic_global_data_1 = JOIN 
    noheader_data                  BY (cd_tipo_trafego) LEFT OUTER, 
    noheader_data_dim_tipo_trafego BY (dim_cd_tipo_trafego);
    
gbic_global_data_2 = JOIN 
    gbic_global_data_1              BY (cd_tipo_cobranca) LEFT OUTER, 
    noheader_data_dim_tipo_cobranca BY (dim_cd_tipo_cobranca);

gbic_global_data_3 = JOIN 
    gbic_global_data_2              BY (cd_situ_chamado_chamador) LEFT OUTER, 
    noheader_data_dim_tipo_cobranca BY (dim_cd_situ_chamado_chamador);

gbic_global_data_4 = JOIN 
    gbic_global_data_3                 BY (cd_sentido_cobranca) LEFT OUTER, 
    noheader_data_dim_sentido_cobranca BY (dim_cd_sentido_cobranca);

gbic_global_data_5 = JOIN 
    gbic_global_data_4                BY (cd_sentido_chamada) LEFT OUTER, 
    noheader_data_dim_sentido_chamada BY (dim_cd_sentido_chamada);

gbic_global_data_6 = JOIN 
    gbic_global_data_5                BY (cd_direcao_chamada) LEFT OUTER, 
    noheader_data_dim_direcao_chamada BY (dim_cd_direcao_chamada);

store_data = FOREACH gbic_global_data_6 {
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
    dim_ds_tipo_cobranca                           AS (ds_tipo_cobranca:         chararray),
    dim_ds_sentido_chamada                         AS (ds_sentido_chamada:       chararray),
    dim_ds_sentido_cobranca                        AS (ds_sentido_cobranca:      chararray),
    dim_ds_tipo_trafego                            AS (ds_tipo_trafego:          chararray),
    dim_ds_direcao_chamada                         AS (ds_direcao_chamada:       chararray),
    dim_ds_situ_chamado_chamador                   AS (ds_situ_chamado_chamador: chararray),
    qt_seg_real                                    AS (qt_seg_real:              int),
    qt_seg_tarifado                                AS (qt_seg_tarifado:          int),
    qt_chamada_intercon                            AS (qt_chamada_intercon:      int),
    (double)REPLACE(vl_bruto_intercon,'\\,','.')   AS (vl_bruto_intercon:        double),
    (double)REPLACE(vl_tarifa_intercon,'\\,','.')  AS (vl_tarifa_intercon:       double),
    (double)REPLACE(vl_liquido_intercon,'\\,','.') AS (vl_liquido_intercon:      double),
    (double)REPLACE(vl_imposto_intercon,'\\,','.') AS (vl_imposto_intercon:      double),
    201                                            AS (gbic_op_id:               int),
    '2015-04-01'                                   AS (month:                    chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_INTERCON'
    USING org.apache.hcatalog.pig.HCatStorer();
