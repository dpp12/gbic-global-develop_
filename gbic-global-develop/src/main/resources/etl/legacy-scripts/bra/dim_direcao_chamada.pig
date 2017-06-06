/* dim_direcao_chamada.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_DIRECAO_CHAMADA/month=$ominalTime/DIM_DIRECAO_CHAMADA-$ominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cd_direcao_chamada: int,
        ds_direcao_chamada: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_direcao_chamada!='DS_CIRECAO_CHAMADA';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'      AS (gbic_op_name:       chararray),
    cd_direcao_chamada AS (cd_direcao_chamada: int),
    ds_direcao_chamada AS (ds_direcao_chamada: chararray),
    201                AS (gbic_op_id:         int),
    '$ominalTime'      AS (month:              chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_DIRECAO_CHAMADA'
    USING org.apache.hcatalog.pig.HCatStorer();
