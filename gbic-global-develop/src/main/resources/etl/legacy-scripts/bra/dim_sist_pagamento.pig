/* dim_sist_pagamento.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_SIST_PAGAMENTO/month=$nominalTime/DIM_SIST_PAGAMENTO-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_sist_pagamento: int,
        ds_sist_pagamento: chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY ds_sist_pagamento!='DS_SIST_PAGAMENTO';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'     AS (gbic_op_name:      chararray),
    id_sist_pagamento AS (id_sist_pagamento: int),
    ds_sist_pagamento AS (ds_sist_pagamento: chararray),
    201               AS (gbic_op_id:        int),
    '$nominalTime'    AS (month:             chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_SIST_PAGAMENTO'
    USING org.apache.hcatalog.pig.HCatStorer();
