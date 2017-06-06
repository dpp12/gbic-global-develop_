/* test_f_tariff_plan.pig
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

in_data = LOAD '/user/gplatform/inbox/{bra}/MSv5/F_TARIFF_PLAN/month=2015-08-01/*'
    USING PigStorage('|')
    AS (country_id:       int,
        month_id:         chararray,
        subscription_id:  chararray,
        tariff_plan_id:   chararray,
        tariff_plan_desc: chararray
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

store_data = FOREACH gbic_filter_obs {
  GENERATE
    gbic_op_name            AS (gbic_op_name:     chararray),
    subscription_id         AS (subscription_id:  chararray),
    tariff_plan_id          AS (tariff_plan_id:   chararray),
    tariff_plan_desc        AS (tariff_plan_desc: chararray),
    gbic_op_id              AS (gbic_op_id:       int),
    '2015-08-01'            AS (month:            chararray);
}

STORE store_data INTO 'gbic_global_staging.gbic_global_f_tariff_plan'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
