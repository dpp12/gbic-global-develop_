/* trafego_dados.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/TRAFEGO_DADOS/month=$nominalTime/TRAFEGO_DADOS-$nominalTime.csv'
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

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(mes, 0, 4), '-'), SUBSTRING(mes, 4, 6));
  GENERATE
    'VIVO BRASIL'           AS (gbic_op_name:        chararray),
    CONCAT(month_id, '-01') AS (mes:                 chararray),
    customer_id             AS (customer_id:         int),
    msisdn                  AS (msisdn:              long),
    id_sist_pgto            AS (id_sist_pgto:        int),
    tipo_linea              AS (tipo_linea:          chararray),
    id_day_type             AS (id_day_type:         int),
    id_time_slot            AS (id_time_slot:        int),
    qt_mbyte_total          AS (qt_mbyte_total:      int),
    num_sessiones_datos     AS (num_sessiones_datos: int), 
    201                     AS (gbic_op_id:          int),
    '$nominalTime'          AS (month:               chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_TRAFEGO_DADOS'
    USING org.apache.hcatalog.pig.HCatStorer();
