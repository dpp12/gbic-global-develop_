/* m_lines.pig
 * -----------
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

/* Organizative segment homogeneization:
 * yyyy-MM-dd,1,ORGANIZATION SEGMENT,1,1,GRANDES CUENTAS,1,Great Corporations
 * yyyy-MM-dd,1,ORGANIZATION SEGMENT,1,2,MEDIANA EMPRESA,2,Medium Business
 * ...
 * yyyy-MM-dd,1,ORGANIZATION SEGMENT,2,EMP,EMPRESAS,1,Great Corporations
 * yyyy-MM-dd,1,ORGANIZATION SEGMENT,2,TEM,T-EMPRESAS,1,Great Corporations
 * ...
 * yyyy-MM-dd,1,ORGANIZATION SEGMENT,201,RES,RESIDENCIAIS GRAN PUBLICO - TOP,6,Consumer
 * ...
 */
segment_org = LOAD '{{ cluster.service }}/homog/month=$nominalTime/dim=1/*'
    USING PigStorage(',')
    AS (seg_month:        chararray,
        seg_concept_id:   int,
        seg_concept_name: chararray,
        seg_gbic_op_id:   int,
        seg_local_cd:     chararray,
        seg_local_name:   chararray,
        seg_global_id:    int,
        seg_global_name:  chararray
    );

tac_file = LOAD '{{ cluster.common }}/tacs/month=$nominalTime/*'
    USING PigStorage('|')
    AS (tac:                chararray,
        volume:             long,
        technology_4g_sp:   chararray,
        technology_4g_br:   chararray,
        technology_4g_mx:   chararray,
        technology_4g_ch:   chararray,
        technology_4g_ur:   chararray,
        technology_4g_pe:   chararray,
        technology_4g_ar:   chararray,
        des_manufact:       chararray,
        des_brand:          chararray,
        des_model:          chararray,
        market_category:    chararray,
        tef_category:       chararray,
        touchscreen:        chararray,
        keyboard:           chararray,
        os:                 chararray,
        version_os:         chararray,
        technology_2g:      chararray,
        technology_3g:      chararray,
        technology_4g_dl:   chararray,
        technology_4g_ul:   chararray
    );

m_tariff_plan = LOAD '{{ hdfs.inbox }}/$ob/$version/DIM_M_TARIFF_PLAN/month=$nominalTime/*'
    USING PigStorage('|')
    AS (dim_country_id:      int,
        dim_month_id:        chararray,
        dim_tariff_plan_id:  chararray,
        dim_des_plan:        chararray,
        dim_data_tariff_ind: int
    );
    
in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/M_LINES/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:         int,
        month_id:           chararray,
        msisdn_id:          chararray,
        subscription_id:    chararray,
        imsi_id:            chararray,
        customer_id:        chararray,
        mobile_customer_id: chararray,
        party_type_cd:      long,
        activation_dt:      chararray,
        prod_type_cd:       chararray,
        imei_num:           chararray,
        line_status_cd:     chararray,
        segment_cd:         chararray,
        pre_post_id:        chararray,
        account_id:         chararray,
        tariff_plan_id:     chararray,
        billing_cycle_id:   chararray,
        postal_cd:          chararray,
        multisim_ind:       long,
        exceed_ind:         long,
        data_tariff_ind:    long,
        extra_data_num:     long,
        extra_data_rv:      double,
        extra_data_qt:      long,
        ppu_num:            long,
        ppu_rv:             double,
        ppu_qt:             long,
        data_consumed_qt:   double,
        data_bundled_qt:    double,
        call_voice_qt:      long,
        voice_consumed_qt:  double,
        sms_consumed_qt:    long,
        prepaid_top_up_id:  long,
        top_up_cost_num:    long,
        top_up_cost_rv:     double,
        top_up_promo_num:   long,
        top_up_promo_rv:    double,
        no_top_up_rv:       double,
        total_rv:           double,
        bta_ind:            int
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

unique_tac_file   = DISTINCT tac_file;
noheader_tac_file = FILTER unique_tac_file BY tac!='TAC';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

gbic_global_data_segment = JOIN
    gbic_filter_obs BY (country_id, UPPER(segment_cd)) LEFT OUTER,
    segment_org     BY (seg_gbic_op_id, seg_local_cd);

/* In ARG, imei_num have 14 digits (or less), while in the rest of OBs they have 15 (or less).
 * We add a 15th digit (zero) at the end of all ARG's imeis and then
 * we fill with zeroes to the left to 15.
 */
gbic_global_data_segment2 = FOREACH gbic_global_data_segment {
    imei = ((long)imei_num IS NULL?
             '-1':
             (imei_num == '-1'?
               imei_num:
               (country_id == 2? SPRINTF('%015d', ((long)SUBSTRING(imei_num, 0, 14) * 10L)): SPRINTF('%015d', (long)imei_num))
             )
           );
  GENERATE
    imei,
    *;
}

gbic_global_data_tac = JOIN
    gbic_global_data_segment2 BY (SUBSTRING(imei, 0, 8)) LEFT OUTER,
    noheader_tac_file         BY (tac);

gbic_global_data_tariff = JOIN
    gbic_global_data_tac BY (country_id, month_id, tariff_plan_id) LEFT OUTER,
    m_tariff_plan        BY (dim_country_id, dim_month_id, dim_tariff_plan_id);

store_data = FOREACH gbic_global_data_tariff {
    technology = (tac IS NULL?
                    'UNKNOWN':
                    (technology_4g_dl == 'No LTE'? (technology_3g == 'No'? '2G': '3G'): '4G')
                 );
  GENERATE
    gbic_op_name                                                   AS (gbic_op_name:       chararray),
    gbic_op_currency                                               AS (currency:           chararray),
    msisdn_id                                                      AS (msisdn_id:          chararray),
    subscription_id                                                AS (subscription_id:    chararray),
    imsi_id                                                        AS (imsi_id:            chararray),
    customer_id                                                    AS (customer_id:        chararray),
    mobile_customer_id                                             AS (mobile_customer_id: chararray),
    party_type_cd                                                  AS (party_type_cd:      long),
    activation_dt                                                  AS (activation_dt:      chararray),
    prod_type_cd                                                   AS (prod_type_cd:       chararray),
    (long)imei                                                     AS (imei_num:           long),
    ((int)tac IS NULL? -1: (int)tac)                               AS (tac_id:             int),
    (des_manufact IS NULL? 'UNKNOWN': des_manufact)                AS (des_manufact:       chararray),
    (des_model IS NULL? 'UNKNOWN': des_model)                      AS (des_model:          chararray),
    (market_category IS NULL? 'UNKNOWN': market_category)          AS (market_category:    chararray),
    (tef_category IS NULL? 'UNKNOWN': tef_category)                AS (tef_category:       chararray),
    (os IS NULL? 'UNKNOWN': os)                                    AS (os:                 chararray),
    (version_os IS NULL? 'UNKNOWN': version_os)                    AS (version_os:         chararray),
    technology                                                     AS (technology:         chararray),
    line_status_cd                                                 AS (line_status_cd:     chararray),
    segment_cd                                                     AS (seg_local_cd:       chararray),
    (seg_local_name IS NULL? 'UNKNOWN': seg_local_name)            AS (seg_local_name:     chararray),
    (seg_global_id IS NULL? -1: seg_global_id)                     AS (seg_global_id:      int),
    (seg_global_name IS NULL? 'UNKNOWN': seg_global_name)          AS (seg_global_name:    chararray),
    pre_post_id                                                    AS (pre_post_id:        chararray),
    account_id                                                     AS (account_id:         chararray),
    tariff_plan_id                                                 AS (tariff_plan_id:     chararray),
    (dim_des_plan IS NULL? 'UNKNOWN': dim_des_plan)                AS (tariff_plan_des:    chararray),
    billing_cycle_id                                               AS (billing_cycle_id:   chararray),
    postal_cd                                                      AS (postal_cd:          chararray),
    multisim_ind                                                   AS (multisim_ind:       long),
    exceed_ind                                                     AS (exceed_ind:         long),
    data_tariff_ind                                                AS (data_tariff_ind:    long),
    extra_data_num                                                 AS (extra_data_num:     long),
    extra_data_rv                                                  AS (extra_data_rv:      double),
    extra_data_qt                                                  AS (extra_data_qt:      long),
    ppu_num                                                        AS (ppu_num:            long),
    ppu_rv                                                         AS (ppu_rv:             double),
    ppu_qt                                                         AS (ppu_qt:             long),
    data_consumed_qt                                               AS (data_consumed_qt:   double),
    data_bundled_qt                                                AS (data_bundled_qt:    double),
    call_voice_qt                                                  AS (call_voice_qt:      long),
    voice_consumed_qt                                              AS (voice_consumed_qt:  double),
    sms_consumed_qt                                                AS (sms_consumed_qt:    long),
    prepaid_top_up_id                                              AS (prepaid_top_up_id:  long),
    top_up_cost_num                                                AS (top_up_cost_num:    long), 
    top_up_cost_rv                                                 AS (top_up_cost_rv:     double),
    top_up_promo_num                                               AS (top_up_promo_num:   long),
    top_up_promo_rv                                                AS (top_up_promo_rv:    double),
    no_top_up_rv                                                   AS (no_top_up_rv:       double),
    total_rv                                                       AS (total_rv:           double),
    bta_ind                                                        AS (bta_ind:            int),
    gbic_op_id                                                     AS (gbic_op_id:         int),
    '$nominalTime'                                                 AS (month:              chararray);
}

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_m_lines'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
