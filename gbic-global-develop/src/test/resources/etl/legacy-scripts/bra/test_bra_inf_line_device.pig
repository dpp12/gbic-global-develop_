/* inf_line_device.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/INF_LINE_DEVICE/month=2015-04-01/INF_LINE_DEVICE-2015-04-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:          chararray,
        msisdn:            long,
        customer_id:       int,
        imei:              long,
        segment_id:        int,
        age_id:            chararray,
        id_pltf:           int,
        line_status:       int,
        id_plan:           int,
        uf_state:          int,
        cod_region:        int,
        activation_date:   int,
        nr_cep:            int,
        qt_calls:          int,
        id_sist_pagamento: int,
        id_tipo_carteira:  int,
        id_segm_negocio:   int,
        nr_tlfn:           long
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

dim_pagamento = LOAD '/user/gplatform/inbox/bra/DIM_SIST_PAGAMENTO/month=2015-06-01/DIM_SIST_PAGAMENTO-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_sist_pagamento_: int,
        ds_sist_pagamento: chararray
        );

unique_data_dim_pag   = DISTINCT dim_pagamento;
noheader_data_dim_pag = FILTER unique_data_dim_pag   BY ds_sist_pagamento!='DS_SIST_PAGAMENTO';

dim_plataforma = LOAD '/user/gplatform/inbox/bra/DIM_PLATAFORMA/month=2015-06-01/DIM_PLATAFORMA-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_pltf_:  int,
        des_pltf: chararray
        );

unique_data_dim_pltf   = DISTINCT dim_plataforma;
noheader_data_dim_pltf = FILTER unique_data_dim_pltf   BY des_pltf!='DES_PLTF';

gbic_global_data_dim = JOIN 
    noheader_data         BY (id_sist_pagamento) LEFT OUTER, 
    noheader_data_dim_pag BY (id_sist_pagamento_);

gbic_global_data = JOIN 
    gbic_global_data_dim   BY (id_pltf) LEFT OUTER, 
    noheader_data_dim_pltf BY (id_pltf_);

store_data = FOREACH gbic_global_data {
    month_id          = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'             AS (gbic_op_name:      chararray),
    CONCAT(month_id, '-01')   AS (id_month:          chararray),
    msisdn                    AS (msisdn:            long),
    customer_id               AS (customer_id:       int),
    0                         AS (fl_multivivo:      int),
    ''                        AS (upselling:         chararray),
    ''                        AS (tp_terminal:       chararray),
    imei                      AS (imei:              long),
    segment_id                AS (segment_id:        int),
    age_id                    AS (age_id:            chararray),
    id_pltf                   AS (id_pltf:           int),
    des_pltf                  AS (des_pltf:          chararray),
    line_status               AS (line_status:       int),
    id_plan                   AS (id_plan:           int),
    uf_state                  AS (uf_state:          chararray),
    cod_region                AS (cod_region:        int),
    activation_date           AS (activation_date:   chararray),
    nr_cep                    AS (nr_cep:            int),
    qt_calls                  AS (qt_calls:          int),
    id_sist_pagamento         AS (id_sist_pagamento: int),
    ds_sist_pagamento         AS (ds_sist_pagamento: chararray),
    id_tipo_carteira          AS (id_tipo_carteira:  int),
    ''                        AS (tp_mtrl:           chararray),
    0                         AS (fl_prqe_int:       int),
    0                         AS (id_fatura:         int),
    ''                        AS (valida_plan:       chararray),
    ''                        AS (tit_dep:           chararray),
    ''                        AS (tipo_plan:         chararray),
    ''                        AS (classe_plan:       chararray),
    ''                        AS (frqa_pl_int:       chararray),
    0                         AS (frqa_tot_ftra:     int),
    id_segm_negocio           AS (id_segm_negocio:   int),
    nr_tlfn                   AS (nr_tlfn:           long),
    201                       AS (gbic_op_id:        int),
    '2015-04-01'              AS (month:             chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_INF_LINE_DEVICE'
    USING org.apache.hcatalog.pig.HCatStorer();
