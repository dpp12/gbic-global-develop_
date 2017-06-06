/* dim_sentido_cobranca.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/{bra}/DIM_SENTIDO_COBRANCA/month=$nominalTime/DIM_SENTIDO_COBRANCA-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cd_sentido_cobranca: int,
        ds_sentido_cobranca: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_sentido_cobranca!='DS_SENTIDO_COBRANCA';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'       AS (gbic_op_name:        chararray),
    cd_sentido_cobranca AS (cd_sentido_cobranca: int),
    ds_sentido_cobranca AS (ds_sentido_cobranca: chararray),
    201                 AS (gbic_op_id:          int),
    '$nominalTime'      AS (month:               chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SENTIDO_COBRANCA'
    USING org.apache.hcatalog.pig.HCatStorer();
