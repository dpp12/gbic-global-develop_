/* ventas_imei.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/VENTAS_IMEI/month=2015-06-01/VENTAS_IMEI-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_lnha:           long,
        cd_area_rgst:      int,
        dt_mvmt_lnha:      chararray,
        id_pltf:           int,
        ds_pltf:           chararray,
        id_tipo_mvmt_lnha: int,
        ds_tipo_mvmt_lnha: chararray,
        id_area_rgnl:      int,
        nm_area_rgnl:      chararray,
        id_tipo_crtr:      int,
        ds_tipo_crtr:      chararray,
        gnro_vndr:         chararray,
        cep:               chararray,
        rede:              chararray,
        ds_plno:           chararray,
        sgmt_plno:         chararray,
        ap_cd_cnl_dstr:    int,
        ap_nm_cnl_dstr:    chararray,
        ap_nr_sral:        long,
        ap_vl_nota_fscl:   double,
        nome_comercial:    chararray,
        tecnologia:        chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY cep!='CEP';

store_data = FOREACH noheader_data GENERATE
    'VIVO BRASIL'                                             AS (gbic_op_name:      chararray),
    id_lnha                                                   AS (id_lnha:           long),
    cd_area_rgst                                              AS (cd_area_rgst:      int),
    ToString(ToDate(dt_mvmt_lnha,'dd/MM/YYYY'), 'YYYY-MM-dd') AS (dt_mvmt_lnha:      chararray),
    id_pltf                                                   AS (id_pltf:           int),
    ds_pltf                                                   AS (ds_pltf:           chararray),
    id_tipo_mvmt_lnha                                         AS (id_tipo_mvmt_lnha: int),
    ds_tipo_mvmt_lnha                                         AS (ds_tipo_mvmt_lnha: chararray),
    id_area_rgnl                                              AS (id_area_rgnl:      int),
    nm_area_rgnl                                              AS (nm_area_rgnl:      chararray),
    id_tipo_crtr                                              AS (id_tipo_crtr:      int),
    ds_tipo_crtr                                              AS (ds_tipo_crtr:      chararray),
    gnro_vndr                                                 AS (gnro_vndr:         chararray),
    cep                                                       AS (cep:               chararray),
    rede                                                      AS (rede:              chararray),
    ds_plno                                                   AS (ds_plno:           chararray),
    sgmt_plno                                                 AS (sgmt_plno:         chararray),
    ap_cd_cnl_dstr                                            AS (ap_cd_cnl_dstr:    int),
    ap_nm_cnl_dstr                                            AS (ap_nm_cnl_dstr:    chararray),
    ap_nr_sral                                                AS (ap_nr_sral:        long),
    ap_vl_nota_fscl                                           AS (ap_vl_nota_fscl:   double),
    nome_comercial                                            AS (nome_comercial:    chararray),
    tecnologia                                                AS (tecnologia:        chararray),
    201                                                       AS (gbic_op_id:        int),
    '2015-06-01'                                              AS (month:             chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_VENTAS_IMEI'
    USING org.apache.hcatalog.pig.HCatStorer();
