/* imei_sales.pig
 * --------------
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

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/IMEI_SALES/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:              int,
        month_id:                chararray,
        msisdn_id:               chararray,
        imei_num:                chararray,
        pre_post_id:             chararray,
        segment_cd:              chararray,
        activation_movement:     chararray,
        tariff_plan_id:          chararray,
        channel_cd:              chararray,
        sales_network_cd:        chararray,
        distribution_channel_cd: chararray,
        campain_cd:              chararray,
        sale_price:              double,
        purchase_price:          double,
        financial_support:       double,
        postal_cd:               chararray,
        device_name:             chararray,
        imei_origin:             chararray,
        subscription_id:         chararray
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

store_data = FOREACH gbic_global_data_segment {
    imei = ((long)imei_num IS NULL?
             '-1':
             (imei_num == '-1'?
               imei_num:
               (country_id == 2? SPRINTF('%015d', ((long)SUBSTRING(imei_num, 0, 14) * 10L)): imei_num)
             )
           );
  GENERATE
    gbic_op_name                                          AS (gbic_op_name:            chararray),
    msisdn_id                                             AS (msisdn_id:               chararray),
    (long)imei                                            AS (imei_num:                long),
    pre_post_id                                           AS (pre_post_id:             chararray),
    segment_cd                                            AS (seg_local_cd:            chararray),
    (seg_local_name IS NULL? 'UNKNOWN': seg_local_name)   AS (seg_local_name:          chararray),
    (seg_global_id IS NULL? -1: seg_global_id)            AS (seg_global_id:           int),
    (seg_global_name IS NULL? 'UNKNOWN': seg_global_name) AS (seg_global_name:         chararray),
    activation_movement                                   AS (activation_movement:     chararray),
    tariff_plan_id                                        AS (tariff_plan_id:          chararray),
    channel_cd                                            AS (channel_cd:              chararray),
    sales_network_cd                                      AS (sales_network_cd:        chararray),
    distribution_channel_cd                               AS (distribution_channel_cd: chararray),
    campain_cd                                            AS (campain_cd:              chararray),
    sale_price                                            AS (sale_price:              double),
    purchase_price                                        AS (purchase_price:          double),
    financial_support                                     AS (financial_support:       double),
    postal_cd                                             AS (postal_cd:               chararray),
    device_name                                           AS (device_name:             chararray),
    imei_origin                                           AS (imei_origin:             chararray),
    subscription_id                                       AS (subscription_id:         chararray),
    gbic_op_id                                            AS (gbic_op_id:              int),
    '$nominalTime'                                        AS (month:                   chararray);
}

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_imei_sales'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
