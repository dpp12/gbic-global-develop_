/* test_f_lines.pig
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
gbic_op_ids = LOAD '/user/gbic/common/GBICGlobalOperators.csv'
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
segment_org = LOAD '/user/gbic/services/gplatform/global/homog/month=2016-01-01/dim=1/*'
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

f_voice_tariff_plan = LOAD '/user/gplatform/inbox/{esp}/MSv5/DIM_F_TARIFF_PLAN/month=2016-01-01/*'
    USING PigStorage('|')
    AS (vt_country_id:      int,
        vt_month_id:        chararray,
        vt_tariff_plan_id:  chararray,
        vt_tariff_plan_des: chararray
    );

f_bband_tariff_plan = LOAD '/user/gplatform/inbox/{esp}/MSv5/DIM_F_TARIFF_PLAN/month=2016-01-01/*'
    USING PigStorage('|')
    AS (bbt_country_id:      int,
        bbt_month_id:        chararray,
        bbt_tariff_plan_id:  chararray,
        bbt_tariff_plan_des: chararray
    );

f_tv_tariff_plan = LOAD '/user/gplatform/inbox/{esp}/MSv5/DIM_F_TARIFF_PLAN/month=2016-01-01/*'
    USING PigStorage('|')
    AS (tvt_country_id:      int,
        tvt_month_id:        chararray,
        tvt_tariff_plan_id:  chararray,
        tvt_tariff_plan_des: chararray
    );

in_data = LOAD '/user/gplatform/inbox/{esp}/MSv5/F_LINES/month=2016-01-01/*'
    USING PigStorage('|')
    AS (country_id:             int,
        month_id:               chararray,
        subscription_id:        chararray,
        administrator_id:       chararray,
        customer_id:            chararray,
        fix_customer_id:        chararray,
        postal_cd:              chararray,
        party_type_cd:          long,
        segment_cd:             chararray,
        voice_ind:              int,
        voice_activation_dt:    chararray,
        voice_type_cd:          chararray,
        voice_tariff_plan_id:   chararray,
        voice_month_rv:         double,
        bband_ind:              int,
        bband_activation_dt:    chararray,
        bband_type_cd:          chararray,
        bband_tariff_plan_id:   chararray,
        speed_band_qt:          int,
        bband_month_rv:         double,
        tv_ind:                 int,
        tv_sales_dt:            chararray,
        tv_activation_dt:       chararray,
        tv_use_dt:              chararray,
        tv_promo_id:            int,
        tv_end_promo_dt:        chararray,
        tv_type_cd:             chararray,
        tv_tariff_plan_id:      chararray,
        tv_points_qt:           int,
        tv_recurring_rv:        double,
        tv_non_recurring_rv:    double,
        tv_month_rv:            double,
        workstation_ind:        int,
        workstation_type_cd:    chararray,
        app_ind:                int,
        total_month_rv:         double,
        data_consumed_qt:       double
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

gbic_global_data_segment = JOIN
    gbic_filter_obs BY (country_id, UPPER(segment_cd)) LEFT OUTER,
    segment_org     BY (seg_gbic_op_id, seg_local_cd);

gbic_voice_tariff = JOIN
    gbic_global_data_segment BY (country_id, month_id, voice_tariff_plan_id) LEFT OUTER,
    f_voice_tariff_plan      BY (vt_country_id, vt_month_id, vt_tariff_plan_id);

gbic_bband_tariff = JOIN
    gbic_voice_tariff   BY (country_id, month_id, bband_tariff_plan_id) LEFT OUTER,
    f_bband_tariff_plan BY (bbt_country_id, bbt_month_id, bbt_tariff_plan_id);

gbic_tv_tariff = JOIN
    gbic_bband_tariff BY (country_id, month_id, tv_tariff_plan_id) LEFT OUTER,
    f_tv_tariff_plan  BY (tvt_country_id, tvt_month_id, tvt_tariff_plan_id);

store_data = FOREACH gbic_tv_tariff GENERATE
    gbic_op_name                                                  AS (gbic_op_name:          chararray),
    gbic_op_currency                                              AS (currency:              chararray),
    subscription_id                                               AS (subscription_id:       chararray),
    administrator_id                                              AS (administrator_id:      chararray),
    customer_id                                                   AS (customer_id:           chararray),
    fix_customer_id                                               AS (fix_customer_id:       chararray),
    postal_cd                                                     AS (postal_cd:             chararray),
    party_type_cd                                                 AS (party_type_cd:         long),
    segment_cd                                                    AS (seg_local_cd:          chararray),
    (seg_local_name IS NULL? 'UNKNOWN': seg_local_name)           AS (seg_local_name:        chararray),
    (seg_global_id IS NULL? -1: seg_global_id)                    AS (seg_global_id:         int),
    (seg_global_name IS NULL? 'UNKNOWN': seg_global_name)         AS (seg_global_name:       chararray),
    voice_ind                                                     AS (voice_ind:             int),
    voice_activation_dt                                           AS (voice_activation_dt:   chararray),
    voice_type_cd                                                 AS (voice_type_cd:         chararray),
    voice_tariff_plan_id                                          AS (voice_tariff_plan_id:  chararray),
    (vt_tariff_plan_des IS NULL? 'UNKNOWN': vt_tariff_plan_des)   AS (voice_tariff_plan_des: chararray),
    voice_month_rv                                                AS (voice_month_rv:        double),
    bband_ind                                                     AS (bband_ind:             int),
    bband_activation_dt                                           AS (bband_activation_dt:   chararray),
    bband_type_cd                                                 AS (bband_type_cd:         chararray),
    bband_tariff_plan_id                                          AS (bband_tariff_plan_id:  chararray),
    (bbt_tariff_plan_des IS NULL? 'UNKNOWN': bbt_tariff_plan_des) AS (bband_tariff_plan_des: chararray),
    speed_band_qt                                                 AS (speed_band_qt:         int),
    bband_month_rv                                                AS (bband_month_rv:        double),
    tv_ind                                                        AS (tv_ind:                int),
    tv_sales_dt                                                   AS (tv_sales_dt:           chararray),
    tv_activation_dt                                              AS (tv_activation_dt:      chararray),
    tv_use_dt                                                     AS (tv_use_dt:             chararray),
    tv_promo_id                                                   AS (tv_promo_id:           int),
    tv_end_promo_dt                                               AS (tv_end_promo_dt:       chararray),
    tv_type_cd                                                    AS (tv_type_cd:            chararray),
    tv_tariff_plan_id                                             AS (tv_tariff_plan_id:     chararray),
    (tvt_tariff_plan_des IS NULL? 'UNKNOWN': tvt_tariff_plan_des) AS (tv_tariff_plan_des:    chararray),
    tv_points_qt                                                  AS (tv_points_qt:          int),
    tv_recurring_rv                                               AS (tv_recurring_rv:       double),
    tv_non_recurring_rv                                           AS (tv_non_recurring_rv:   double),
    tv_month_rv                                                   AS (tv_month_rv:           double),
    workstation_ind                                               AS (workstation_ind:       int),
    workstation_type_cd                                           AS (workstation_type_cd:   chararray),
    app_ind                                                       AS (app_ind:               int),
    total_month_rv                                                AS (total_month_rv:        double),
    data_consumed_qt                                              AS (data_consumed_qt:      double),
    gbic_op_id                                                    AS (gbic_op_id:            int),
    '2016-01-01'                                                  AS (month:                 chararray);

STORE store_data INTO 'gbic_global_staging.gbic_global_f_lines'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
