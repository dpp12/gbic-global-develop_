/* trafego.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/TRAFEGO/month=2015-06-01/TRAFEGO-2015-06-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_month:                     chararray,
        msisdn:                       long,
        id_day_type:                  int,
        id_time_slot:                 int,
        id_tipo_origem:               int,
        vl_franquia_voz_out:          chararray,
        vl_franquia_voz_in:           chararray,
        seg_llamadas_bonus_out:       chararray,
        seg_llamadas_bonus_in:        chararray,
        seg_llamadas_franquia_out:    chararray,
        seg_llamadas_franquia_in:     chararray,
        num_llamadas_franquia_out:    int,
        num_llamadas_franquia_in:     int,
        seg_llamadas_out:             chararray,
        seg_llamadas_in:              chararray,
        num_llamadas_out:             int,
        num_llamadas_in:              int,
        ingresos_liquido_llamada_out: chararray,
        ingresos_liquido_llamada_in:  chararray,
        ingresos_total_llamada_out:   chararray,
        ingresos_total_llamada_in:    chararray,
        num_franquia_sms_out:         int,
        num_franquia_sms_in:          int,
        num_sms_out:                  int,
        num_sms_in:                   int,
        ingresos_sms_out:             chararray,
        ingresos_sms_in:              chararray,
        num_franquia_mms_out:         int,
        num_franquia_mms_in:          int,
        num_mms_out:                  int,
        num_mms_in:                   int,
        ingresos_mms_out:             chararray,
        ingresos_mms_in:              chararray,
        num_byte_franquia_out:        int,
        num_byte_franquia_in:         int,
        num_byte_total_out:           int,
        num_byte_total_in:            int
        );
        
unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_month!='ID_MONTH';

store_data = FOREACH noheader_data {
    month_id       = CONCAT(CONCAT(SUBSTRING(id_month, 0, 4), '-'), SUBSTRING(id_month, 4, 6));
  GENERATE
    'VIVO BRASIL'                                           AS (gbic_op_name:                 chararray),
    CONCAT(month_id, '-01')                                 AS (id_month:                     chararray),
    msisdn                                                  AS (msisdn:                       long),
    id_day_type                                             AS (id_day_type:                  int),
    id_time_slot                                            AS (id_time_slot:                 int),
    id_tipo_origem                                          AS (id_tipo_origem:               int),
    (double)REPLACE(vl_franquia_voz_out,'\\,','.')          AS (vl_franquia_voz_out:          double),
    (double)REPLACE(vl_franquia_voz_in,'\\,','.')           AS (vl_franquia_voz_in:           double),
    (double)REPLACE(seg_llamadas_bonus_out,'\\,','.')       AS (seg_llamadas_bonus_out:       double),
    (double)REPLACE(seg_llamadas_bonus_in,'\\,','.')        AS (seg_llamadas_bonus_in:        double),
    (double)REPLACE(seg_llamadas_franquia_out,'\\,','.')    AS (seg_llamadas_franquia_out:    double),
    (double)REPLACE(seg_llamadas_franquia_in,'\\,','.')     AS (seg_llamadas_franquia_in:     double),
    num_llamadas_franquia_out                               AS (num_llamadas_franquia_out:    int),
    num_llamadas_franquia_in                                AS (num_llamadas_franquia_in:     int),
    (double)REPLACE(seg_llamadas_out,'\\,','.')             AS (seg_llamadas_out:             double),
    (double)REPLACE(seg_llamadas_in,'\\,','.')              AS (seg_llamadas_in:              double),
    num_llamadas_out                                        AS (num_llamadas_out:             int),
    num_llamadas_in                                         AS (num_llamadas_in:              int),
    (double)REPLACE(ingresos_liquido_llamada_out,'\\,','.') AS (ingresos_liquido_llamada_out: double),
    (double)REPLACE(ingresos_liquido_llamada_in,'\\,','.')  AS (ingresos_liquido_llamada_in:  double),
    (double)REPLACE(ingresos_total_llamada_out,'\\,','.')   AS (ingresos_total_llamada_out:   double),
    (double)REPLACE(ingresos_total_llamada_in,'\\,','.')    AS (ingresos_total_llamada_in:    double),
    num_franquia_sms_out                                    AS (num_franquia_sms_out:         int),
    num_franquia_sms_in                                     AS (num_franquia_sms_in:          int),
    num_sms_out                                             AS (num_sms_out:                  int),
    num_sms_in                                              AS (num_sms_in:                   int),
    (double)REPLACE(ingresos_sms_out,'\\,','.')             AS (ingresos_sms_out:             double),
    (double)REPLACE(ingresos_sms_in,'\\,','.')              AS (ingresos_sms_in:              double),
    num_franquia_mms_out                                    AS (num_franquia_mms_out:         int),
    num_franquia_mms_in                                     AS (num_franquia_mms_in:          int),
    num_mms_out                                             AS (num_mms_out:                  int),
    num_mms_in                                              AS (num_mms_in:                   int),
    (double)REPLACE(ingresos_mms_out,'\\,','.')             AS (ingresos_mms_out:             double),
    (double)REPLACE(ingresos_mms_in,'\\,','.')              AS (ingresos_mms_in:              double),
    num_byte_franquia_out                                   AS (num_byte_franquia_out:        int),
    num_byte_franquia_in                                    AS (num_byte_franquia_in:         int),
    num_byte_total_out                                      AS (num_byte_total_out:           int),
    num_byte_total_in                                       AS (num_byte_total_in:            int),
    201                                                     AS (gbic_op_id:                   int),
    '2015-06-01'                                            AS (month:                        chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_TRAFEGO'
    USING org.apache.hcatalog.pig.HCatStorer();
