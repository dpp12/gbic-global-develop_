/* test_traffic_voice.pig
 * ----------------------
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
gbic_op_ids = LOAD '/user/gbic/common/GBICGlobalOperators.csv'
    USING PigStorage(',')
    AS (gbic_op_id:       int,
        gbic_op_name:     chararray,
        gbic_op_cd1:      chararray,
        gbic_op_cd2:      chararray,
        gbic_op_currency: chararray
    );

billing_cycle = LOAD '/user/gplatform/inbox/{esp}/MSv5/DIM_M_BILLING_CYCLE/*'
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

in_data = LOAD '/user/gplatform/inbox/{esp}/MSv5/TRAFFIC_VOICE/month=2015-01-01/*'
    USING PigStorage('|')
    AS (country_id:                 int,
        month_id:                   chararray,
        billing_cycle_dt:           chararray,
        billing_cycle_id:           chararray,
        subscription_id:            chararray,
        msisdn_id:                  chararray,
        day_cd:                     int,
        time_range_cd:              int,
        imei_num:                   long,
        call_offnet_fixed_out:      int,
        call_onnet_fixed_out:       int,
        call_offnet_mobile_out:     int,
        call_onnet_mobile_out:      int,
        call_international_out:     int,
        call_onnet_out_free:        int,
        call_onnet_rcm_out:         int,
        call_roaming_out:           int,
        call_out_special_numbers:   int,
        call_fixed_in:              int,
        call_offnet_mobile_in:      int,
        call_onnet_mobile_in:       int,
        call_roaming_in:            int,
        call_international_in:      int,
        min_offnet_fixed_out:       double,
        min_onnet_fixed_out:        double,
        min_offnet_mobile_out:      double,
        min_onnet_mobile_out:       double,
        min_international_out:      double,
        min_onnet_free_out:         double,
        min_onnet_rcm_out:          double,
        min_roaming_out:            double,
        min_out_special_numbers:    double,
        min_fixed_in:               double,
        min_offnet_mobile_in:       double,
        min_onnet_mobile_in:        double,
        min_roaming_in:             double,
        min_international_in:       double,
        min_fixed_out_bundled:      double,
        min_mobile_out_bundled:     double,
        min_fixed_out_not_bundled:  double,
        min_mobile_out_not_bundled: double,
        min_fixed_out_exceed:       double,
        min_mobile_out_exceed:      double,
        roaming_rv:                 double,
        out_other_rv:               double,
        out_national_onnet_rv:      double,
        out_national_offnet_rv:     double,
        out_national_fixed_rv:      double,
        out_international_rv:       double,
        call_2g_out:                int,
        call_3g_out:                int,
        call_4g_out:                int,
        call_2g_in:                 int,
        call_3g_in:                 int,
        call_4g_in:                 int,
        min_2g_out:                 double,
        min_3g_out:                 double,
        min_4g_out:                 double,
        min_2g_in:                  double,
        min_3g_in:                  double,
        min_4g_in:                  double
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

store_data = FOREACH gbic_global_billing_cycle GENERATE
    gbic_op_name                                                     AS (gbic_op_name:               chararray),
    billing_cycle_id                                                 AS (billing_cycle_id:           chararray),
    (billing_cycle_dt == '190001'?
        'Not Available':
        (bill_cycle_des IS NULL? 'UNKNOWN': bill_cycle_des))         AS (billing_cycle_des:          chararray),
    (bill_cycle_start_dt IS NULL? '1900-01-01': bill_cycle_start_dt) AS (billing_cycle_start_dt:     chararray),
    (bill_cycle_end_dt IS NULL? '1900-01-01': bill_cycle_end_dt)     AS (billing_cycle_end_dt:       chararray),
    (bill_due_dt IS NULL? '1900-01-01': bill_due_dt)                 AS (billing_due_dt:             chararray),
    (bill_rv_computes IS NULL? 0: bill_rv_computes)                  AS (billing_rv_computes:        int),
    subscription_id                                                  AS (subscription_id:            chararray),
    msisdn_id                                                        AS (msisdn_id:                  chararray),
    day_cd                                                           AS (day_cd:                     int),
    time_range_cd                                                    AS (time_range_cd:              int),
    imei_num                                                         AS (imei_num:                   long),
    call_offnet_fixed_out                                            AS (call_offnet_fixed_out:      int),
    call_onnet_fixed_out                                             AS (call_onnet_fixed_out:       int),
    call_offnet_mobile_out                                           AS (call_offnet_mobile_out:     int),
    call_onnet_mobile_out                                            AS (call_onnet_mobile_out:      int),
    call_international_out                                           AS (call_international_out:     int),
    call_onnet_out_free                                              AS (call_onnet_out_free:        int),
    call_onnet_rcm_out                                               AS (call_onnet_rcm_out:         int),
    call_roaming_out                                                 AS (call_roaming_out:           int),
    call_out_special_numbers                                         AS (call_out_special_numbers:   int),
    call_fixed_in                                                    AS (call_fixed_in:              int),
    call_offnet_mobile_in                                            AS (call_offnet_mobile_in:      int),
    call_onnet_mobile_in                                             AS (call_onnet_mobile_in:       int),
    call_roaming_in                                                  AS (call_roaming_in:            int),
    call_international_in                                            AS (call_international_in:      int),
    min_offnet_fixed_out                                             AS (min_offnet_fixed_out:       double),
    min_onnet_fixed_out                                              AS (min_onnet_fixed_out:        double),
    min_offnet_mobile_out                                            AS (min_offnet_mobile_out:      double),
    min_onnet_mobile_out                                             AS (min_onnet_mobile_out:       double),
    min_international_out                                            AS (min_international_out:      double),
    min_onnet_free_out                                               AS (min_onnet_free_out:         double),
    min_onnet_rcm_out                                                AS (min_onnet_rcm_out:          double),
    min_roaming_out                                                  AS (min_roaming_out:            double),
    min_out_special_numbers                                          AS (min_out_special_numbers:    double),
    min_fixed_in                                                     AS (min_fixed_in:               double),
    min_offnet_mobile_in                                             AS (min_offnet_mobile_in:       double),
    min_onnet_mobile_in                                              AS (min_onnet_mobile_in:        double),
    min_roaming_in                                                   AS (min_roaming_in:             double),
    min_international_in                                             AS (min_international_in:       double),
    min_fixed_out_bundled                                            AS (min_fixed_out_bundled:      double),
    min_mobile_out_bundled                                           AS (min_mobile_out_bundled:     double),
    min_fixed_out_not_bundled                                        AS (min_fixed_out_not_bundled:  double),
    min_mobile_out_not_bundled                                       AS (min_mobile_out_not_bundled: double),
    min_fixed_out_exceed                                             AS (min_fixed_out_exceed:       double),
    min_mobile_out_exceed                                            AS (min_mobile_out_exceed:      double),
    roaming_rv                                                       AS (roaming_rv:                 double),
    out_other_rv                                                     AS (out_other_rv:               double),
    out_national_onnet_rv                                            AS (out_national_onnet_rv:      double),
    out_national_offnet_rv                                           AS (out_national_offnet_rv:     double),
    out_national_fixed_rv                                            AS (out_national_fixed_rv:      double),
    out_international_rv                                             AS (out_international_rv:       double),
    call_2g_out                                                      AS (call_2g_out:                int),
    call_3g_out                                                      AS (call_3g_out:                int),
    call_4g_out                                                      AS (call_4g_out:                int),
    call_2g_in                                                       AS (call_2g_in:                 int),
    call_3g_in                                                       AS (call_3g_in:                 int),
    call_4g_in                                                       AS (call_4g_in:                 int),
    min_2g_out                                                       AS (min_2g_out:                 double),
    min_3g_out                                                       AS (min_3g_out:                 double),
    min_4g_out                                                       AS (min_4g_out:                 double),
    min_2g_in                                                        AS (min_2g_in:                  double),
    min_3g_in                                                        AS (min_3g_in:                  double),
    min_4g_in                                                        AS (min_4g_in:                  double),
    gbic_op_id                                                       AS (gbic_op_id:                 int),
    '2015-01-01'                                                     AS (month:                      chararray);

STORE store_data INTO 'gbic_global_staging.gbic_global_traffic_voice'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
