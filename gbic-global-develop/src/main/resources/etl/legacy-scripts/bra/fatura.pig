/* fatura.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/FATURA/month=$nominalTime/FATURA-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:          chararray,
        msisdn:            long,
        customer_id:       int,
        fecha_inicio_fact: chararray,
        fecha_final_fact:  chararray,
        agregacion_fact:   chararray,
        id_factura:        int,
        segundos_llamadas: int,
        numero_llamadas:   int,
        numero_byte_total: long,
        ingresos_total:    chararray,
        ingresos_neto:     chararray
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'                             AS (gbic_op_name:      chararray),
    CONCAT(month_id, '-01')                   AS (id_month:          chararray),
    msisdn                                    AS (msisdn:            long),
    customer_id                               AS (customer_id:       int),
    fecha_inicio_fact                         AS (fecha_inicio_fact: chararray),
    fecha_final_fact                          AS (fecha_final_fact:  chararray),
    agregacion_fact                           AS (agregacion_fact:   chararray),
    id_factura                                AS (id_factura:        int),
    segundos_llamadas                         AS (segundos_llamadas: int),
    numero_llamadas                           AS (numero_llamadas:   int),
    numero_byte_total                         AS (numero_byte_total: long),
    (double)REPLACE(ingresos_total,'\\,','.') AS (ingresos_total:    double),
    (double)REPLACE(ingresos_neto,'\\,','.')  AS (ingresos_neto:     double),
    201                                       AS (gbic_op_id:        int),
    '$nominalTime'                            AS (month:             chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_FATURA'
    USING org.apache.hcatalog.pig.HCatStorer();
