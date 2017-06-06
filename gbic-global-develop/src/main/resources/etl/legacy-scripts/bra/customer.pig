/* customer.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/CUSTOMER/month=$nominalTime/CUSTOMER-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:                 chararray,
        id_cli:                   long,
        id_tipo_carteira:         int,
        ds_grpo_sgmt_cli:         chararray,
        ds_oprc:                  chararray,
        ds_prsc:                  chararray,
        ds_grpo_prdt_cli_pre:     chararray,
        ds_sub_grpo_prdt_cli_pre: chararray,
        id_sexo:                  chararray,
        dt_alta_cli:              chararray,
        ds_agng_cli:              chararray,
        dt_nscm:                  chararray,
        ds_clss_cnae:             chararray,
        qt_cli:                   chararray,
        qt_lnha_mvel_pos:         int,
        qt_lnha_mvel_pre:         int,
        qt_lnha_mvel_cntl:        int,
        qt_lnha_fwt_pos:          int,
        qt_lnha_fwt_pre:          int,
        qt_lnha_fwt_cntl:         int,
        qt_lnha_fixa_pos:         int,
        qt_lnha_fixa_pre:         int,
        qt_tv_dth_sd:             int,
        qt_tv_dth_hd:             int,
        qt_tv_fbra_sd:            int,
        qt_tv_fbra_hd:            int,
        qt_tv_cabo_sd:            int,
        qt_tv_cabo_hd:            int,
        qt_vivo_play:             int,
        qt_bnda_lrga_fbra:        int,
        qt_bnda_lrga_cabo:        int,
        qt_bnda_lrga_adsl:        int,
        qt_bnda_lrga_vdsl:        int,
        qt_plca_3g_pos:           int,
        qt_plca_3g_pre:           int,
        qt_plca_4g_pos:           int,
        qt_plca_4g_pre:           int,
        qt_plca_fwt_pos:          int,
        qt_plca_fwt_pre:          int,
        qt_plca_fwt_cntl:         int,
        qt_m2m_3g:                int,
        qt_m2m_4g:                int,
        qt_pdti:                  int,
        qt_prqe_ddos:             int,
        qt_jntr_tlsp:             int,
        qt_rmal_tlsp:             int,
        qt_trnc_tlsp:             int,
        qt_a_tlcm_ada:            int,
        qt_a_tlcm_vox:            int,
        qt_a_tlcm_e1:             int,
        qt_rcrg:                  int
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

in_data_join = LOAD '/user/gplatform/inbox/bra/DIM_CUSTOMER/month=$nominalTime/DIM_CUSTOMER-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_cli_:          long,
        customer_id_mov: long,
        customer_id_fjo: long
        );
        
unique_data_join   = DISTINCT in_data_join;

gbic_data_added = JOIN
    noheader_data    BY (id_cli) LEFT OUTER,
    unique_data_join BY (id_cli_);

store_data = FOREACH gbic_data_added {
     month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'               AS (gbic_op_name:             chararray),
    CONCAT(month_id, '-01')     AS (id_month:                 chararray),
    id_cli                      AS (id_cli:                   long),
    customer_id_mov             AS (customer_id_mov:          long),
    customer_id_fjo             AS (customer_id_fjo:          long),
    id_tipo_carteira            AS (id_tipo_carteira:         int),
    ds_grpo_sgmt_cli            AS (ds_grpo_sgmt_cli:         chararray),
    ds_oprc                     AS (ds_oprc:                  chararray),
    ds_prsc                     AS (ds_prsc:                  chararray),
    ds_grpo_prdt_cli_pre        AS (ds_grpo_prdt_cli_pre:     chararray),
    ds_sub_grpo_prdt_cli_pre    AS (ds_sub_grpo_prdt_cli_pre: chararray),
    id_sexo                     AS (id_sexo:                  chararray),
    dt_alta_cli                 AS (dt_alta_cli:              chararray),
    ds_agng_cli                 AS (ds_agng_cli:              chararray),
    dt_nscm                     AS (dt_nscm:                  chararray),
    ds_clss_cnae                AS (ds_clss_cnae:             chararray),
    qt_cli                      AS (qt_cli:                   chararray),
    qt_lnha_mvel_pos            AS (qt_lnha_mvel_pos:         int),
    qt_lnha_mvel_pre            AS (qt_lnha_mvel_pre:         int),
    qt_lnha_mvel_cntl           AS (qt_lnha_mvel_cntl:        int),
    qt_lnha_fwt_pos             AS (qt_lnha_fwt_pos:          int),
    qt_lnha_fwt_pre             AS (qt_lnha_fwt_pre:          int),
    qt_lnha_fwt_cntl            AS (qt_lnha_fwt_cntl:         int),
    qt_lnha_fixa_pos            AS (qt_lnha_fixa_pos:         int),
    qt_lnha_fixa_pre            AS (qt_lnha_fixa_pre:         int),
    qt_tv_dth_sd                AS (qt_tv_dth_sd:             int),
    qt_tv_dth_hd                AS (qt_tv_dth_hd:             int),
    qt_tv_fbra_sd               AS (qt_tv_fbra_sd:            int),
    qt_tv_fbra_hd               AS (qt_tv_fbra_hd:            int),
    qt_tv_cabo_sd               AS (qt_tv_cabo_sd:            int),
    qt_tv_cabo_hd               AS (qt_tv_cabo_hd:            int),
    qt_vivo_play                AS (qt_vivo_play:             int),
    qt_bnda_lrga_fbra           AS (qt_bnda_lrga_fbra:        int),
    qt_bnda_lrga_cabo           AS (qt_bnda_lrga_cabo:        int),
    qt_bnda_lrga_adsl           AS (qt_bnda_lrga_adsl:        int),
    qt_bnda_lrga_vdsl           AS (qt_bnda_lrga_vdsl:        int),
    qt_plca_3g_pos              AS (qt_plca_3g_pos:           int),
    qt_plca_3g_pre              AS (qt_plca_3g_pre:           int),
    qt_plca_4g_pos              AS (qt_plca_4g_pos:           int),
    qt_plca_4g_pre              AS (qt_plca_4g_pre:           int),
    qt_plca_fwt_pos             AS (qt_plca_fwt_pos:          int),
    qt_plca_fwt_pre             AS (qt_plca_fwt_pre:          int),
    qt_plca_fwt_cntl            AS (qt_plca_fwt_cntl:         int),
    qt_m2m_3g                   AS (qt_m2m_3g:                int),
    qt_m2m_4g                   AS (qt_m2m_4g:                int),
    qt_pdti                     AS (qt_pdti:                  int),
    qt_prqe_ddos                AS (qt_prqe_ddos:             int),
    qt_jntr_tlsp                AS (qt_jntr_tlsp:             int),
    qt_rmal_tlsp                AS (qt_rmal_tlsp:             int),
    qt_trnc_tlsp                AS (qt_trnc_tlsp:             int),
    qt_a_tlcm_ada               AS (qt_a_tlcm_ada:            int),
    qt_a_tlcm_vox               AS (qt_a_tlcm_vox:            int),
    qt_a_tlcm_e1                AS (qt_a_tlcm_e1:             int),
    qt_rcrg                     AS (qt_rcrg:                  int),
    201                         AS (gbic_op_id:               int),
    '$nominalTime'              AS (month:                    chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_CUSTOMER'
    USING org.apache.hcatalog.pig.HCatStorer();
