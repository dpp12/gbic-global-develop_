/* dim_tipo_trafego.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_TIPO_TRAFEGO/month=$nominalTime/DIM_TIPO_TRAFEGO-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cd_tipo_trafego: int,
        ds_tipo_trafego: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_tipo_trafego!='DS_TIPO_TRAFEGO';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'   AS (gbic_op_name:    chararray),
    cd_tipo_trafego AS (cd_tipo_trafego: int),
    ds_tipo_trafego AS (ds_tipo_trafego: chararray),
    201             AS (gbic_op_id:      int),
    '$nominalTime'  AS (month:           chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_TIPO_TRAFEGO'
    USING org.apache.hcatalog.pig.HCatStorer();
