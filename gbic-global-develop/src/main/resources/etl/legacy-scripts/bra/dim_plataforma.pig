/* dim_plataforma.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_PLATAFORMA/month=$nominalTime/DIM_PLATAFORMA-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_pltf:  int,
        des_pltf: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY des_pltf!='DES_PLTF';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'  AS (gbic_op_name: chararray),
    id_pltf        AS (id_pltf:      int),
    des_pltf       AS (des_pltf:     chararray),
    201            AS (gbic_op_id:   int),
    '$nominalTime' AS (month:        chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_PLATAFORMA'
    USING org.apache.hcatalog.pig.HCatStorer();
