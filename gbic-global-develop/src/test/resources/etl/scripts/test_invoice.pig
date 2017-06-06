/* test_invoice.pig
 * ------------------
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

billing = LOAD '/user/gplatform/inbox/{arg}/MSv5/DIM_M_BILLING_CYCLE/month=2015-04-01/*'
    USING PigStorage('|')
    AS (dim_country_id:        int,
        dim_month_id:          chararray,
        dim_billing_cycle_id:  chararray,
        dim_des_billing_cycle: chararray
    );

in_data = LOAD '/user/gplatform/inbox/{arg}/MSv5/INVOICE/month=2015-04-01/*'
    USING PigStorage('|')
    AS (country_id:         int,
        month_id:           chararray,
        customer_id:        chararray,
        msisdn_id:          chararray,
        activation_dt:      chararray,
        billing_cycle_id:   chararray,
        quota_data_rv:      double,
        quota_voice_rv:     double,
        quota_mess_rv:      double,
        quota_agg_rv:       double,
        traffic_data_rv:    double,
        traffic_voice_rv:   double,
        traffic_mess_rv:    double,
        traffic_agg_rv:     double,
        roaming_rv:         double,
        sva_rv:             double,
        packs_rv:           double,
        top_up_ex_rv:       double,
        top_up_co_rv:       double,
        gb_camp_rv:         double,
        others_rv:          double,
        tot_rv:             double,
        top_up_rv:          double,
        itx_rv:             double,
        exp_itx_rv:         double,
        total_invoice_rv:   double,
        subscription_id:    chararray
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

unique_data_billing   = DISTINCT billing;
noheader_data_billing = FILTER unique_data_billing BY dim_month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_global_billing = JOIN
    gbic_global_data      BY (billing_cycle_id) LEFT OUTER,
    noheader_data_billing BY (dim_billing_cycle_id);

gbic_filter_obs = FILTER gbic_global_billing BY gbic_op_id IS NOT NULL;

store_data = FOREACH gbic_filter_obs {
  GENERATE
    gbic_op_name                                                      AS (gbic_op_name:      chararray),
    customer_id                                                       AS (customer_id:       chararray),
    msisdn_id                                                         AS (msisdn_id:         chararray),
    activation_dt                                                     AS (activation_dt:     chararray),
    billing_cycle_id                                                  AS (billing_cycle_id:  chararray),
    (dim_des_billing_cycle IS NULL? 'UNKNOWN': dim_des_billing_cycle) AS (billing_cycle_des: chararray),
    quota_data_rv                                                     AS (quota_data_rv:     double),
    quota_voice_rv                                                    AS (quota_voice_rv:    double),
    quota_mess_rv                                                     AS (quota_mess_rv:     double),
    quota_agg_rv                                                      AS (quota_agg_rv:      double),
    traffic_data_rv                                                   AS (traffic_data_rv:   double),
    traffic_voice_rv                                                  AS (traffic_voice_rv:  double),
    traffic_mess_rv                                                   AS (traffic_mess_rv:   double),
    traffic_agg_rv                                                    AS (traffic_agg_rv:    double),
    roaming_rv                                                        AS (roaming_rv:        double),
    sva_rv                                                            AS (sva_rv:            double),
    packs_rv                                                          AS (packs_rv:          double),
    top_up_ex_rv                                                      AS (top_up_ex_rv:      double),
    top_up_co_rv                                                      AS (top_up_co_rv:      double),
    gb_camp_rv                                                        AS (gb_camp_rv:        double),
    others_rv                                                         AS (others_rv:         double),
    tot_rv                                                            AS (tot_rv:            double),
    top_up_rv                                                         AS (top_up_rv:         double),
    itx_rv                                                            AS (itx_rv:            double),
    exp_itx_rv                                                        AS (exp_itx_rv:        double),
    total_invoice_rv                                                  AS (total_invoice_rv:  double),
    subscription_id                                                   AS (subscription_id:   chararray),
    gbic_op_id                                                        AS (gbic_op_id:        int),
    '2015-04-01'                                                      AS (month:             chararray);
}

STORE store_data INTO 'gbic_global_staging.gbic_global_invoice'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
