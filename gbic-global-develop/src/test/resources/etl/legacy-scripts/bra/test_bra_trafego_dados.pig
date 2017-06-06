/* trafego_dados.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/TRAFEGO_DADOS/month=2015-03-01/TRAFEGO_DADOS-2015-03-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (mes:                 chararray,
        customer_id:         int,
        msisdn:              long,
        id_sist_pgto:        int,
        tipo_linea:          chararray,
        id_day_type:         int,
        id_time_slot:        int,
        qt_mbyte_total:      int,
        num_sessiones_datos: int
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY mes!='MES';

dim_sist_pagamento = LOAD '/user/bottomup/inbox/{bra}/DIM_SIST_PAGAMENTO/month=2015-06-01/DIM_SIST_PAGAMENTO-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_sist_pagamento: int,
        ds_sist_pagamento: chararray
        );

unique_data_dim   = DISTINCT dim_sist_pagamento;
noheader_data_dim = FILTER unique_data_dim BY ds_sist_pagamento!='DS_SIST_PAGAMENTO';

gbic_global_data = JOIN 
    noheader_data     BY (id_sist_pgto) LEFT OUTER, 
    noheader_data_dim BY (id_sist_pagamento);
    
store_data = FOREACH gbic_global_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(mes, 0, 4), '-'), SUBSTRING(mes, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name:        chararray),
    CONCAT(month_id, '-01') AS (mes:                 chararray),
    customer_id             AS (customer_id:         int),
    msisdn                  AS (msisdn:              long),
    id_sist_pgto            AS (id_sist_pgto:        int),
    ds_sist_pagamento       AS (ds_sist_pgto:        chararray),
    tipo_linea              AS (tipo_linea:          chararray),
    id_day_type             AS (id_day_type:         int),
    id_time_slot            AS (id_time_slot:        int),
    qt_mbyte_total          AS (qt_mbyte_total:      int),
    num_sessiones_datos     AS (num_sessiones_datos: int), 
    201                     AS (gbic_op_id:          int),
    '2015-03-01'            AS (month:               chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_TRAFEGO_DADOS'
    USING org.apache.hcatalog.pig.HCatStorer();
