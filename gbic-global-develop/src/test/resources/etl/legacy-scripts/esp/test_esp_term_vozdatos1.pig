/* term_vozdatos1.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/esp/TERM_VOZDATOS1/month=2015-05-01/TERM_VOZDATOS1-2015-05-01.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_dia_mes:   chararray,
        msisdn:       long,
        imei:         long,
        vol_black:    double,
        vol_chat:     double,
        vol_emocion:  double,
        vol_internet: double,
        vol_intranet: double,
        vol_m_emp:    double,
        vol_m_prof:   double,
        vol_navega:   double,
        vol_wifi:     double,
        vol_resto:    double
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY id_dia_mes!='ID_DIA_MES';

store_data = FOREACH noheader_data {
     mes = CONCAT(CONCAT(CONCAT(SUBSTRING(id_dia_mes, 0, 4), '-'), CONCAT(SUBSTRING(id_dia_mes, 4, 6), '-')),SUBSTRING(id_dia_mes, 6, 8));
  GENERATE
    'MOVISTAR ESPAÑA' AS (gbic_op_name: chararray),
    mes               AS (id_dia_mes:   chararray),
    msisdn            AS (msisdn:       long),
    imei              AS (imei:         long),
    vol_black         AS (vol_black:    double),
    vol_chat          AS (vol_chat:     double),
    vol_emocion       AS (vol_emocion:  double),
    vol_internet      AS (vol_internet: double),
    vol_intranet      AS (vol_intranet: double),
    vol_m_emp         AS (vol_m_emp:    double),
    vol_m_prof        AS (vol_m_prof:   double),
    vol_navega        AS (vol_navega:   double),
    vol_wifi          AS (vol_wifi:     double),
    vol_resto         AS (vol_resto:    double),
    1                 AS (gbic_op_id:   int),
    '2015-05-01'      AS (month:        chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_ESP_TERM_VOZDATOS1'
    USING org.apache.hcatalog.pig.HCatStorer();
