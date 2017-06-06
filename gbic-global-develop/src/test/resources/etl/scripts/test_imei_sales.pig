/* test_imei_sales.pig
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
segment_org = LOAD '/user/gbic/services/gplatform/global/homog/month=2015-05-01/dim=1/*'
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

in_data = LOAD '/user/gplatform/inbox/{per}/MSv5/IMEI_SALES/month=2015-01-01/*'
    USING PigStorage('|')
    AS (country_id:              int,
        month_id:                chararray,
        msisdn_id:               chararray,
        imei_num:                long,
        pre_post_id:             chararray,
        segment_cd:              chararray,
        activation_movement:     chararray,
        tariff_plan_id:          chararray,
        channel_cd:              chararray,
        sales_network_cd:        chararray,
        distribution_channel_cd: chararray,
        campain_cd:              chararray,
        sale_price:              double,
        purchase_price:          int,
        financial_support:       int,
        postal_cd:               int,
        device_name:             chararray,
        imei_origin:             chararray
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
  GENERATE
    gbic_op_name                                          AS (gbic_op_name:            chararray),
    msisdn_id                                             AS (msisdn_id:               chararray),
    imei_num                                              AS (imei_num:                long),
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
    purchase_price                                        AS (purchase_price:          int),
    financial_support                                     AS (financial_support:       int),
    postal_cd                                             AS (postal_cd:               int),
    device_name                                           AS (device_name:             chararray),
    imei_origin                                           AS (imei_origin:             chararray),
    gbic_op_id                                            AS (gbic_op_id:              int),
    '2015-01-01'                                          AS (month:                   chararray);
}

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_IMEI_SALES'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
