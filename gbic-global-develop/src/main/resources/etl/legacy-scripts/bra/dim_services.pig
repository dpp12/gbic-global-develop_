/* dim_services.pig
 * ------------------
 * 
 */
 
in_data = LOAD '/user/gplatform/inbox/bra/DIM_SERVICES/month=$nominalTime/DIM_SERVICES-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_srvc:             chararray,
        cd_srvc:             chararray,
        ds_srvc:             chararray,
        dt_ini_srvc:         chararray,
        dt_fim_srvc:         chararray,
        ds_clsf_srvc:        chararray,
        ds_tipo_srvc:        chararray,
        nm_unde_rgnl:        chararray,
        ds_pltf:             chararray,
        nm_sist_orig:        chararray,
        qt_frqa:             chararray,
        ds_unde_mdda_ftrm:   chararray,
        ds_tipo_cntr_fdld:   chararray,
        ds_tipo_crtr:        chararray,
        ds_fmla_srvc:        chararray,
        ds_ctga_srvc:        chararray,
        ds_grpo_srvc:        chararray,
        ds_sub_grpo_srvc:    chararray,
        ds_sub_grpo_pf_srvc: chararray,
        pntc_pj:             chararray,
        pntc_pf:             chararray,
        dt_insr_dw:          chararray,
        dt_atlz_dw:          chararray,
        fl_pct_extra:        int,
        prco_bruto:          chararray,
        prco_liquido:        chararray

        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_srvc!='ID_SRVC';

store_data = FOREACH noheader_data GENERATE
    'BRA'                                   AS (gbic_op_name:        chararray),
    (int)id_srvc                            AS (id_srvc:             int),
    cd_srvc                                 AS (cd_srvc:             chararray),
    ds_srvc                                 AS (ds_srvc:             chararray),
    dt_ini_srvc                             AS (dt_ini_srvc:         chararray),
    dt_fim_srvc                             AS (dt_fim_srvc:         chararray),
    ds_clsf_srvc                            AS (ds_clsf_srvc:        chararray),
    ds_tipo_srvc                            AS (ds_tipo_srvc:        chararray),
    nm_unde_rgnl                            AS (nm_unde_rgnl:        chararray),
    ds_pltf                                 AS (ds_pltf:             chararray),
    nm_sist_orig                            AS (nm_sist_orig:        chararray),
    qt_frqa                                 AS (qt_frqa:             chararray),
    ds_unde_mdda_ftrm                       AS (ds_unde_mdda_ftrm:   chararray),
    ds_tipo_cntr_fdld                       AS (ds_tipo_cntr_fdld:   chararray),
    ds_tipo_crtr                            AS (ds_tipo_crtr:        chararray),
    ds_fmla_srvc                            AS (ds_fmla_srvc:        chararray),
    ds_ctga_srvc                            AS (ds_ctga_srvc:        chararray),
    ds_grpo_srvc                            AS (ds_grpo_srvc:        chararray),
    ds_sub_grpo_srvc                        AS (ds_sub_grpo_srvc:    chararray),
    ds_sub_grpo_pf_srvc                     AS (ds_sub_grpo_pf_srvc: chararray),
    (double)REPLACE(pntc_pj,'\\,','.')      AS (pntc_pj:             double),
    (double)REPLACE(pntc_pf,'\\,','.')      AS (pntc_pf:             double),
    dt_insr_dw                              AS (dt_insr_dw:          chararray),
    dt_atlz_dw                              AS (dt_atlz_dw:          chararray),
    fl_pct_extra                            AS (fl_pct_extra:        int),
    (double)REPLACE(prco_bruto,'\\,','.')   AS (prco_bruto:          double),
    (double)REPLACE(prco_liquido,'\\,','.') AS (prco_liquido:        double),
    201                                     AS (gbic_op_id:          int),
    '$nominalTime'                          AS (month:               chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SERVICES'
    USING org.apache.hcatalog.pig.HCatStorer();
