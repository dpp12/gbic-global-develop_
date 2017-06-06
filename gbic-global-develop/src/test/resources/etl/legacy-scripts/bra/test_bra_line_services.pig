/* line_services.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/LINE_SERVICES/month=2015-03-01/LINE_SERVICES-2015-03-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:   chararray,
        msisdn:     long,
        id_service: int
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name: chararray),
    CONCAT(month_id, '-01') AS (id_month:     chararray),
    msisdn                  AS (msisdn:       long),
    id_service              AS (id_service:   int),
    201                     AS (gbic_op_id:   int),
    '2015-03-01'            AS (month:        chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_LINE_SERVICES'
    USING org.apache.hcatalog.pig.HCatStorer();
