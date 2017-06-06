/* cust.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/CUST/month=$nominalTime/CUST-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:         chararray,
        customer_id:      long,
        birth_date:       chararray,
        id_tipo_carteira: int,
        id_segm_negocio:  chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data{
     month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name:             chararray),
    CONCAT(month_id, '-01') AS (id_month:                 chararray),
    (long)-1                AS (id_cli:                   long),
    customer_id             AS (customer_id_mov:          long),
    (long)-1                AS (customer_id_fjo:          long),
    id_tipo_carteira        AS (id_tipo_carteira:         int),
    id_segm_negocio         AS (ds_grpo_sgmt_cli:         chararray),
    ''                      AS (ds_oprc:                  chararray),
    ''                      AS (ds_prsc:                  chararray),
    ''                      AS (ds_grpo_prdt_cli_pre:     chararray),
    ''                      AS (ds_sub_grpo_prdt_cli_pre: chararray),
    ''                      AS (id_sexo:                  chararray),
    ''                      AS (dt_alta_cli:              chararray),
    ''                      AS (ds_agng_cli:              chararray),
    birth_date              AS (dt_nscm:                  chararray),
    ''                      AS (ds_clss_cnae:             chararray),
    ''                      AS (qt_cli:                   chararray),
    0                       AS (qt_lnha_mvel_pos:         int),
    0                       AS (qt_lnha_mvel_pre:         int),
    0                       AS (qt_lnha_mvel_cntl:        int),
    0                       AS (qt_lnha_fwt_pos:          int),
    0                       AS (qt_lnha_fwt_pre:          int),
    0                       AS (qt_lnha_fwt_cntl:         int),
    0                       AS (qt_lnha_fixa_pos:         int),
    0                       AS (qt_lnha_fixa_pre:         int),
    0                       AS (qt_tv_dth_sd:             int),
    0                       AS (qt_tv_dth_hd:             int),
    0                       AS (qt_tv_fbra_sd:            int),
    0                       AS (qt_tv_fbra_hd:            int),
    0                       AS (qt_tv_cabo_sd:            int),
    0                       AS (qt_tv_cabo_hd:            int),
    0                       AS (qt_vivo_play:             int),
    0                       AS (qt_bnda_lrga_fbra:        int),
    0                       AS (qt_bnda_lrga_cabo:        int),
    0                       AS (qt_bnda_lrga_adsl:        int),
    0                       AS (qt_bnda_lrga_vdsl:        int),
    0                       AS (qt_plca_3g_pos:           int),
    0                       AS (qt_plca_3g_pre:           int),
    0                       AS (qt_plca_4g_pos:           int),
    0                       AS (qt_plca_4g_pre:           int),
    0                       AS (qt_plca_fwt_pos:          int),
    0                       AS (qt_plca_fwt_pre:          int),
    0                       AS (qt_plca_fwt_cntl:         int),
    0                       AS (qt_m2m_3g:                int),
    0                       AS (qt_m2m_4g:                int),
    0                       AS (qt_pdti:                  int),
    0                       AS (qt_prqe_ddos:             int),
    0                       AS (qt_jntr_tlsp:             int),
    0                       AS (qt_rmal_tlsp:             int),
    0                       AS (qt_trnc_tlsp:             int),
    0                       AS (qt_a_tlcm_ada:            int),
    0                       AS (qt_a_tlcm_vox:            int),
    0                       AS (qt_a_tlcm_e1:             int),
    0                       AS (qt_rcrg:                  int),
    201                     AS (gbic_op_id:               int),
    '$nominalTime'          AS (month:                    chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_CUSTOMER'
    USING org.apache.hcatalog.pig.HCatStorer();
