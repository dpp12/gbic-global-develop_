/* dim_plno.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_PLNO/month=2015-06-01/DIM_PLNO-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_plno:            int,
        cd_plno_sist_orig:  chararray,
        ds_plno:            chararray,
        ds_pltf:            chararray,
        nm_unde_rgnl:       chararray,
        ds_tipo_crtr:       chararray,
        dt_ini_cmcl_plno:   chararray,
        dt_fim_cmcl_plno:   chararray,
        qt_frqa:            int,
        ds_tipo_plno:       chararray,
        ds_tipo_plan_seg:   chararray,
        classe_plan_seg:    chararray,
        tit_dep:            chararray,
        dt_atlz_dw:         chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_plno!='DS_PLNO';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'      AS (gbic_op_name:      chararray),
    id_plno            AS (id_plno:           int),
    cd_plno_sist_orig  AS (cd_plno_sist_orig: chararray),
    ds_plno            AS (ds_plno:           chararray),
    ds_pltf            AS (ds_pltf:           chararray),
    nm_unde_rgnl       AS (nm_unde_rgnl:      chararray),
    ds_tipo_crtr       AS (ds_tipo_crtr:      chararray),
    dt_ini_cmcl_plno   AS (dt_ini_cmcl_plno:  chararray),
    dt_fim_cmcl_plno   AS (dt_fim_cmcl_plno:  chararray),
    qt_frqa            AS (qt_frqa:           int),
    ds_tipo_plno       AS (ds_tipo_plno:      chararray),
    ds_tipo_plan_seg   AS (ds_tipo_plan_seg:  chararray),
    classe_plan_seg    AS (classe_plan_seg:   chararray),
    tit_dep            AS (tit_dep:           chararray),
    dt_atlz_dw         AS (dt_atlz_dw:        chararray),
    201                AS (gbic_op_id:        int),
    '2015-06-01'       AS (month:             chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_PLNO'
    USING org.apache.hcatalog.pig.HCatStorer();
