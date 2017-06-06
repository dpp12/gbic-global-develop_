/* mov_dev.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/MOV_DEV/month=2015-04-01/MOV_DEV-2015-04-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:             chararray,
        msisdn:               long,
        customer_id:          int,
        id_mov:               int,
        mov_channel:          chararray,
        mov_date:             chararray,
        fl_portabilidade:     int,
        numero_do_telefone:   long,
        chave_linha_anterior: long,
        situacao_inicial:     int,
        situacao_final:       int
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

dim_mov_type = LOAD '/user/gplatform/inbox/bra/DIM_MOV_TYPE/month=2015-06-01/DIM_MOV_TYPE-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_mov_: int,
        ds_mov: chararray
        );

unique_data_dim   = DISTINCT dim_mov_type;
noheader_data_dim = FILTER unique_data_dim   BY ds_mov!='DS_MOV';

gbic_global_data = JOIN 
    noheader_data     BY (id_mov) LEFT OUTER, 
    noheader_data_dim BY (id_mov_);

store_data = FOREACH gbic_global_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name:         chararray),
    CONCAT(month_id, '-01') AS (id_month:             chararray),
    msisdn                  AS (msisdn:               long),
    customer_id             AS (customer_id:          int),
    id_mov                  AS (id_mov:               int),
    ds_mov                  AS (ds_mov:               chararray),
    mov_channel             AS (mov_channel:          chararray),
    mov_date                AS (mov_date:             chararray),
    fl_portabilidade        AS (fl_portabilidade:     int),
    numero_do_telefone      AS (numero_do_telefone:   long),
    chave_linha_anterior    AS (chave_linha_anterior: long),
    situacao_inicial        AS (situacao_inicial:     int),
    situacao_final          AS (situacao_final:       int),
    201                     AS (gbic_op_id:           int),
    '2015-04-01'            AS (month:                chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_MOV_DEV'
    USING org.apache.hcatalog.pig.HCatStorer();
