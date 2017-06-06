/* dim_mov_type.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_MOV_TYPE/month=$nominalTime/DIM_MOV_TYPE-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_mov: int,
        ds_mov: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_mov!='DS_MOV';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'  AS (gbic_op_name: chararray),
    id_mov         AS (id_mov:       int),
    ds_mov         AS (ds_mov:       chararray),
    201            AS (gbic_op_id:   int),
    '$nominalTime' AS (month:        chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_MOV_TYPE'
    USING org.apache.hcatalog.pig.HCatStorer();
