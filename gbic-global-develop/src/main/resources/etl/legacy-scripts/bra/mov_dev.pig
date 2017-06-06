/* mov_dev.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/MOV_DEV/month=$nominalTime/MOV_DEV-$nominalTime.csv'
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

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name:         chararray),
    CONCAT(month_id, '-01') AS (id_month:             chararray),
    msisdn                  AS (msisdn:               long),
    customer_id             AS (customer_id:          int),
    id_mov                  AS (id_mov:               int),
    mov_channel             AS (mov_channel:          chararray),
    mov_date                AS (mov_date:             chararray),
    fl_portabilidade        AS (fl_portabilidade:     int),
    numero_do_telefone      AS (numero_do_telefone:   long),
    chave_linha_anterior    AS (chave_linha_anterior: long),
    situacao_inicial        AS (situacao_inicial:     int),
    situacao_final          AS (situacao_final:       int),
    201                     AS (gbic_op_id:           int),
    '$nominalTime'          AS (month:                chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_MOV_DEV'
    USING org.apache.hcatalog.pig.HCatStorer();
