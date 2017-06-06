/* daily_traffic.pig
 * -----------------
 */
/* Global operators:
 * 1,MOVISTAR ESPAÑA,es,esp,EUR
 * 2,MOVISTAR ARGENTINA,ar,arg,ARS
 * ...
 * 5,MOVISTAR PERU,pe,per,PEN
 * ...
 * 201,VIVO BRASIL,br,bra,BRL
 * ...
 */
gbic_op_ids = LOAD '{{ cluster.common }}/GBICGlobalOperators.csv'
    USING PigStorage(',')
    AS (gbic_op_id:       int,
        gbic_op_name:     chararray,
        gbic_op_cd1:      chararray,
        gbic_op_cd2:      chararray,
        gbic_op_currency: chararray
    );

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/DAILY_TRAFFIC/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:       int,
        month_id:         chararray,
        call_dt:          chararray,
        msisdn_id:        chararray,
        imei_num:         chararray,
        day_type_cd:      chararray,
        bank_holiday_cd:  int,
        roaming_cd:       int,
        calls_total_qt:   int,
        minutes_tot_qt:   double,
        sms_total_qt:     int,
        mb_total_qt:      double,
        subscription_id:  chararray
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

store_data = FOREACH gbic_filter_obs {
    imei = ((long)imei_num IS NULL?
             '-1':
             (imei_num == '-1'?
               imei_num:
               (country_id == 2? SPRINTF('%015d', ((long)SUBSTRING(imei_num, 0, 14) * 10L)): imei_num)
             )
           );
  GENERATE
    gbic_op_name                                  AS (gbic_op_name:    chararray),
    call_dt                                       AS (call_dt:         chararray),
    msisdn_id                                     AS (msisdn_id:       chararray),
    (long)imei                                    AS (imei_num:        long),
    day_type_cd                                   AS (day_type_cd:     chararray),
    bank_holiday_cd                               AS (bank_holiday_cd: int),
    roaming_cd                                    AS (roaming_cd:      int),
    calls_total_qt                                AS (calls_total_qt:  int),
    minutes_tot_qt                                AS (minutes_tot_qt:  double),
    sms_total_qt                                  AS (sms_total_qt:    int),
    mb_total_qt                                   AS (mb_total_qt:     double),
    subscription_id                               AS (subscription_id: chararray),
    gbic_op_id                                    AS (gbic_op_id:      int),
    '$nominalTime'                                AS (month:           chararray);
}

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_daily_traffic'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
