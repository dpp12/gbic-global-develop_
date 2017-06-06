/* m_line_services.pig
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

group_sva = LOAD '{{ hdfs.inbox }}/$ob/$version/DIM_M_GROUP_SVA/month=$nominalTime/*'
    USING PigStorage('|')
    AS (gr_country_id: int,
        gr_month_id:   chararray,
        gr_group_sva:  chararray,
        group_sva_des: chararray
    );

services = LOAD '{{ hdfs.inbox }}/$ob/$version/DIM_M_SERVICES/month=$nominalTime/*'
    USING PigStorage('|')
    AS (serv_country_id: int,
        serv_month_id:   chararray,
        serv_id_service: chararray,
        des_service:     chararray
    );

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/M_LINE_SERVICES/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:       int,
        month_id:         chararray,
        msisdn_id:        chararray,
        subscription_id:  chararray,
        activation_dt:    chararray,
        id_service:       chararray,
        operation_cd:     chararray,
        group_sva:        chararray,
        service_activ_dt: chararray,
        recurrent_ind:    int
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

gbic_global_group = JOIN
    gbic_filter_obs BY (country_id, month_id, group_sva) LEFT OUTER,
    group_sva       BY (gr_country_id, gr_month_id, gr_group_sva);

gbic_global_services = JOIN
    gbic_global_group BY (country_id, month_id, id_service) LEFT OUTER,
    services          BY (serv_country_id, serv_month_id, serv_id_service);

store_data = FOREACH gbic_global_services GENERATE
    gbic_op_name                                                       AS (gbic_op_name:     chararray),
    msisdn_id                                                          AS (msisdn_id:        chararray),
    subscription_id                                                    AS (subscription_id:  chararray),
    activation_dt                                                      AS (activation_dt:    chararray),
    id_service                                                         AS (id_service:       chararray),
    (des_service IS NULL? CONCAT('Service ', id_service): des_service) AS (des_service:      chararray),
    operation_cd                                                       AS (operation_cd:     chararray),
    group_sva                                                          AS (group_sva:        chararray),
    (group_sva_des IS NULL? group_sva: group_sva_des)                  AS (group_sva_des:    chararray),
    service_activ_dt                                                   AS (service_activ_dt: chararray),
    recurrent_ind                                                      AS (recurrent_ind:    int),
    gbic_op_id                                                         AS (gbic_op_id:       int),
    '$nominalTime'                                                     AS (month:            chararray);

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_m_line_services'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
