/* 201_inf_line_device.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/201_INF_LINE_DEVICE/month=$nominalTime/201_INF_LINE_DEVICE-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:          chararray,
        msisdn:            long,
        customer_id:       int,
        fl_multivivo:      int,
        upselling:         chararray,
        tp_terminal:       chararray,
        imei:              long,
        segment_id:        int,
        age_id:            chararray,
        id_pltf:           int,
        line_status:       int,
        id_plan:           int,
        uf_state:          chararray,
        cod_region:        int,
        activation_date:   chararray,
        nr_cep:            int,
        qt_calls:          int,
        id_sist_pagamento: int,
        id_tipo_carteira:  int,
        tp_mtrl:           chararray,
        fl_prqe_int:       int,
        id_fatura:         int,
        valida_plan:       chararray,
        tit_dep:           chararray,
        tipo_plan:         chararray,
        classe_plan:       chararray,
        frqa_pl_int:       chararray,
        frqa_tot_ftra:     int,
        id_segm_negocio:   int,
        nr_tlfn:           long
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data {
    month_id          = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name:      chararray),
    CONCAT(month_id, '-01') AS (id_month:          chararray),
    msisdn                  AS (msisdn:            long),
    customer_id             AS (customer_id:       int),
    fl_multivivo            AS (fl_multivivo:      int),
    upselling               AS (upselling:         chararray),
    tp_terminal             AS (tp_terminal:       chararray),
    imei                    AS (imei:              long),
    segment_id              AS (segment_id:        int),
    age_id                  AS (age_id:            chararray),
    id_pltf                 AS (id_pltf:           int),
    line_status             AS (line_status:       int),
    id_plan                 AS (id_plan:           int),
    uf_state                AS (uf_state:          chararray),
    cod_region              AS (cod_region:        int),
    activation_date         AS (activation_date:   chararray),
    nr_cep                  AS (nr_cep:            int),
    qt_calls                AS (qt_calls:          int),
    id_sist_pagamento       AS (id_sist_pagamento: int),
    id_tipo_carteira        AS (id_tipo_carteira:  int),
    tp_mtrl                 AS (tp_mtrl:           chararray),
    fl_prqe_int             AS (fl_prqe_int:       int),
    id_fatura               AS (id_fatura:         int),
    valida_plan             AS (valida_plan:       chararray),
    tit_dep                 AS (tit_dep:           chararray),
    tipo_plan               AS (tipo_plan:         chararray),
    classe_plan             AS (classe_plan:       chararray),
    frqa_pl_int             AS (frqa_pl_int:       chararray),
    frqa_tot_ftra           AS (frqa_tot_ftra:     int),
    id_segm_negocio         AS (id_segm_negocio:   int),
    nr_tlfn                 AS (nr_tlfn:           long),
    201                     AS (gbic_op_id:        int),
    '$nominalTime'          AS (month:             chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_INF_LINE_DEVICE'
    USING org.apache.hcatalog.pig.HCatStorer();
