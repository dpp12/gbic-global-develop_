/* recarga.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/RECARGA/month=2015-04-01/RECARGA-2015-04-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:             chararray,
        msisdn:               long,
        customer_id:          int,
        ingresos_recarga:     chararray,
        num_recarga:          int
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'                               AS (gbic_op_name:     chararray),
    CONCAT(month_id, '-01')                     AS (id_month:         chararray),
    msisdn                                      AS (msisdn:           long),
    customer_id                                 AS (customer_id:      int),
    (double)REPLACE(ingresos_recarga,'\\,','.') AS (ingresos_recarga: double),
    num_recarga                                 AS (num_recarga:      int),
    201                                         AS (gbic_op_id:       int),
    '2015-04-01'                                AS (month:            chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_RECARGA'
    USING org.apache.hcatalog.pig.HCatStorer();
