/* dim_situ_chamado_chamador.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_SITU_CHAMADO_CHAMADOR/month=$nominalTime/DIM_SITU_CHAMADO_CHAMADOR-$nominalTime.csv'
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
    '$nominalTime'           AS (month:                    chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SITU_CHAMADO_CHAMADOR'
    USING org.apache.hcatalog.pig.HCatStorer();
