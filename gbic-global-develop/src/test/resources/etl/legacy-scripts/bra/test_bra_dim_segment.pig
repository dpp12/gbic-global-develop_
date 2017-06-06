/* dim_segment.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_SEGMENT/month=2015-06-01/DIM_SEGMENT-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_segment:  int,
        des_segment: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY des_segment!='DES_SEGMENT';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL' AS (gbic_op_name: chararray),
    id_segment    AS (id_segment:   int),
    des_segment   AS (des_segment:  chararray),
    201           AS (gbic_op_id:   int),
    '2015-06-01'  AS (month:        chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SEGMENT'
    USING org.apache.hcatalog.pig.HCatStorer();
