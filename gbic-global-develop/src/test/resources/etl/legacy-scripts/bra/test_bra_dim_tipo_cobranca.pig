/* dim_tipo_cobranca.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_TIPO_COBRANCA/month=2015-06-01/DIM_TIPO_COBRANCA-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cd_tipo_cobranca: int,
        ds_tipo_cobranca: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_tipo_cobranca!='DS_TIPO_COBRANCA';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'    AS (gbic_op_name:     chararray),
    cd_tipo_cobranca AS (cd_tipo_cobranca: int),
    ds_tipo_cobranca AS (ds_tipo_cobranca: chararray),
    201              AS (gbic_op_id:       int),
    '2015-06-01'     AS (month:            chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_TIPO_COBRANCA'
    USING org.apache.hcatalog.pig.HCatStorer();
