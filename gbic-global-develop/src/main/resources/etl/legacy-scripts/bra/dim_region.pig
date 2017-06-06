/* dim_regions.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_REGION/month=$nomnalTime/DIM_REGION-$nomnalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (cod_region: int,
        des_region: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY des_region!='DES_REGION';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL' AS (gbic_op_name: chararray),
    cod_region    AS (cod_region:   int),
    des_region    AS (des_region:   chararray),
    201           AS (gbic_op_id:   int),
    '$nomnalTime' AS (month:        chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_REGION'
    USING org.apache.hcatalog.pig.HCatStorer();
