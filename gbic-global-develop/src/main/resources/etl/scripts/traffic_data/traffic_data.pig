/* traffic_data.pig
 * ----------------
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

billing_cycle = LOAD '{{ hdfs.inbox }}/$ob/$version/DIM_M_BILLING_CYCLE/*'
    USING PigStorage('|')
    AS (bill_country_id:     int,
        bill_cycle_month:    chararray,
        bill_cycle_id:       chararray,
        bill_cycle_des:      chararray,
        bill_cycle_start_dt: chararray,
        bill_cycle_end_dt:   chararray,
        bill_due_dt:         chararray,
        bill_rv_computes:    int
    );

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/TRAFFIC_DATA/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:             int,
        month_id:               chararray,
        billing_cycle_dt:       chararray,
        billing_cycle_id:       chararray,
        subscription_id:        chararray,
        msisdn_id:              chararray,
        day_cd:                 int,
        time_range_cd:          int,
        imei_num:               chararray,
        total_qt:               double,
        mb_2g_qt:               double,
        mb_3g_qt:               double,
        mb_4g_qt:               double,
        mb_roaming:             double
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

gbic_global_billing_cycle = JOIN
    gbic_filter_obs BY (billing_cycle_dt, billing_cycle_id, country_id) LEFT OUTER,
    billing_cycle   BY (bill_cycle_month, bill_cycle_id, bill_country_id);

store_data = FOREACH gbic_global_billing_cycle {
    imei = ((long)imei_num IS NULL?
             '-1':
             (imei_num == '-1'?
               imei_num:
               (country_id == 2? SPRINTF('%015d', ((long)SUBSTRING(imei_num, 0, 14) * 10L)): imei_num)
             )
           );
  GENERATE
    gbic_op_name                                                     AS (gbic_op_name:           chararray),
    billing_cycle_id                                                 AS (billing_cycle_id:       chararray),
    (billing_cycle_dt == '190001'?
        'Not Available':
        (bill_cycle_des IS NULL? 'UNKNOWN': bill_cycle_des))         AS (billing_cycle_des:      chararray),
    (bill_cycle_start_dt IS NULL? '1900-01-01': bill_cycle_start_dt) AS (billing_cycle_start_dt: chararray),
    (bill_cycle_end_dt IS NULL? '1900-01-01': bill_cycle_end_dt)     AS (billing_cycle_end_dt:   chararray),
    (bill_due_dt IS NULL? '1900-01-01': bill_due_dt)                 AS (billing_due_dt:         chararray),
    (bill_rv_computes IS NULL? 0: bill_rv_computes)                  AS (billing_rv_computes:    int),
    subscription_id                                                  AS (subscription_id:        chararray),
    msisdn_id                                                        AS (msisdn_id:              chararray),
    day_cd                                                           AS (day_cd:                 int),
    time_range_cd                                                    AS (time_range_cd:          int),
    (long)imei                                                       AS (imei_num:               long),
    total_qt                                                         AS (total_qt:               double),
    mb_2g_qt                                                         AS (mb_2g_qt:               double),
    mb_3g_qt                                                         AS (mb_3g_qt:               double),
    mb_4g_qt                                                         AS (mb_4g_qt:               double),
    mb_roaming                                                       AS (mb_roaming:             double),
    gbic_op_id                                                       AS (gbic_op_id:             int),
    '$nominalTime'                                                   AS (month:                  chararray);
}

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_traffic_data'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
