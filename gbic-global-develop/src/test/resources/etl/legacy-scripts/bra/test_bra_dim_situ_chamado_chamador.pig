/* dim_situ_chamado_chamador.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_SITU_CHAMADO_CHAMADOR/month=2015-06-01/DIM_SITU_CHAMADO_CHAMADOR-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cd_situ_chamado_chamador: int,
        ds_situ_chamado_chamador: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_situ_chamado_chamador!='DS_SITU_CHAMADO_CHAMADOR';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'            AS (gbic_op_name:             chararray),
    cd_situ_chamado_chamador AS (cd_situ_chamado_chamador: int),
    ds_situ_chamado_chamador AS (ds_situ_chamado_chamador: chararray),
    201                      AS (gbic_op_id:               int),
    '2015-06-01'             AS (month:                    chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SITU_CHAMADO_CHAMADOR'
    USING org.apache.hcatalog.pig.HCatStorer();
