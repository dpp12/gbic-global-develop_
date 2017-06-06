/* dim_sentido_chamada.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_SENTIDO_CHAMADA/month=$nominalTime/DIM_SENTIDO_CHAMADA-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cd_sentido_chamada: int,
        ds_sentido_chamada: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_sentido_chamada!='DS_SENTIDO_CHAMADA';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'      AS (gbic_op_name:       chararray),
    cd_sentido_chamada AS (cd_sentido_chamada: int),
    ds_sentido_chamada AS (ds_sentido_chamada: chararray),
    201                AS (gbic_op_id:         int),
    '$nominalTime'     AS (month:              chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SENTIDO_CHAMADA'
    USING org.apache.hcatalog.pig.HCatStorer();
