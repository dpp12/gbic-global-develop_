/* handset_upgrade.pig
 * -------------------
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

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/HANDSET_UPGRADE/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:                 int,
        month_id:                   chararray,
        msisdn_id:                  chararray,
        activation_dt:              chararray,
        imei_num:                   chararray,
        hu_dt:                      chararray,
        hu_type_id:                 chararray,
        hu_amount:                  double,
        commission_amount:          int,
        financial_support:          int,
        hu_loyalty_points_balance:  int,
        hu_loyalty_points_redeemed: int
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
    gbic_op_name               AS (gbic_op_name:               chararray),
    msisdn_id                  AS (msisdn_id:                  chararray),
    activation_dt              AS (activation_dt:              chararray),
    (long)imei                 AS (imei_num:                   long),
    hu_dt                      AS (hu_dt:                      chararray),
    hu_type_id                 AS (hu_type_id:                 chararray),
    hu_amount                  AS (hu_amount:                  double),
    commission_amount          AS (commission_amount:          int),
    financial_support          AS (financial_support:          int),
    hu_loyalty_points_balance  AS (hu_loyalty_points_balance:  int),
    hu_loyalty_points_redeemed AS (hu_loyalty_points_redeemed: int),
    gbic_op_id                 AS (gbic_op_id:                 int),
    '$nominalTime'             AS (month:                      chararray);
}

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_handset_upgrade'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
