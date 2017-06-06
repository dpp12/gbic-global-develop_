/* test_movements.pig
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
segment_org = LOAD '/user/gbic/services/gplatform/global/homog/month=2015-01-01/dim=1/*'
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

/* Movement type homogenization:
 * yyyy-MM-dd,3,MOVEMENT TYPE,1,-1,NO APLICA,-1,Not Available
 * yyyy-MM-dd,3,MOVEMENT TYPE,1,AFRA,ALTA FRAUDULENTA,8,Fraudulent Add
 * ...
 * yyyy-MM-dd,3,MOVEMENT TYPE,2,-1,NO INFORMADO,-1,Not Available
 * yyyy-MM-dd,3,MOVEMENT TYPE,2,1,ALTA PURA,1,Pure New Add
 * ...
 * yyyy-MM-dd,3,MOVEMENT TYPE,201,-1,NÃO INFORMADO,-1,Not Available
 * ...
 */
movement_type = LOAD '/user/gbic/services/gplatform/global/homog/month=2015-01-01/dim=3/*'
    USING PigStorage(',')
    AS (mov_tp_month:             chararray,
        mov_tp_concept_id:        int,
        mov_tp_concept_name:      chararray,
        mov_tp_gbic_op_id:        int,
        mov_tp_local_group_id:    chararray,
        mov_tp_local_group_des:   chararray,
        mov_tp_global_group_id:   int,
        mov_tp_global_group_des:  chararray
    );

m_tariff_plan = LOAD '/user/gplatform/inbox/esp/MSv5/DIM_M_TARIFF_PLAN/month=2015-01-01/*'
    USING PigStorage('|')
    AS (tar_country_id:      int,
        tar_month_id:        chararray,
        tar_tariff_plan_id:  chararray,
        tar_des_plan:        chararray,
        tar_data_tariff_ind: int
    );

m_campaign = LOAD '/user/gplatform/inbox/esp/MSv5/DIM_M_CAMPAIGN/month=2015-01-01/*'
    USING PigStorage('|')
    AS (camp_country_id:    int,
        camp_month_id:      chararray,
        camp_campaign_id:   chararray,
        camp_campaign_des:  chararray
    );

m_movement = LOAD '/user/gplatform/inbox/esp/MSv5/DIM_M_MOVEMENT/month=2015-01-01/*'
    USING PigStorage('|')
    AS (mov_country_id:        int,
        mov_month_id:          chararray,
        mov_movement_id:       chararray,
        mov_movement_des:      chararray,
        mov_count_movement_qt: int,
        mov_group_movement_cd: chararray
    );

m_oper = LOAD '/user/gplatform/inbox/esp/MSv5/DIM_M_OPERATORS/month=2015-01-01/*'
    USING PigStorage('|')
    AS (oper_country_id:  int,
        oper_month_id:    chararray,
        oper_port_op_cd:  chararray,
        oper_port_op_des: chararray
    );

in_data = LOAD '/user/gplatform/inbox/esp/MSv5/MOVEMENTS/month=2015-01-01/*'
    USING PigStorage('|')
    AS (country_id:           int,
        month_id:             chararray,
        customer_id:          chararray,
        msisdn_id:            chararray,
        subscription_id:      chararray,
        activation_dt:        chararray,
        movement_id:          chararray,
        movement_dt:          chararray,
        movement_channel_id:  chararray,
        campaign_id:          chararray,
        segment_cd:           chararray,
        pre_post_id:          chararray,
        prev_pre_post_id:     chararray,
        tariff_plan_id:       chararray,
        prev_tariff_plan_id:  chararray,
        prod_type_cd:         chararray,
        port_op_cd:           chararray
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

unique_m_campaign = DISTINCT m_campaign;

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

gbic_global_data_oper = JOIN
    gbic_filter_obs BY (country_id, month_id, port_op_cd) LEFT OUTER,
    m_oper          BY (oper_country_id, oper_month_id, oper_port_op_cd);

gbic_global_data_movement = JOIN
    gbic_global_data_oper BY (country_id, month_id, movement_id) LEFT OUTER,
    m_movement            BY (mov_country_id, mov_month_id, mov_movement_id);

gbic_global_data_movement_tp = JOIN
    gbic_global_data_movement BY (country_id, mov_group_movement_cd) LEFT OUTER,
    movement_type             BY (mov_tp_gbic_op_id, mov_tp_local_group_id);

gbic_global_data_campaign = JOIN
    gbic_global_data_movement_tp BY (country_id, month_id, campaign_id) LEFT OUTER,
    unique_m_campaign            BY (camp_country_id, camp_month_id, camp_campaign_id);

gbic_global_data_segment = JOIN
    gbic_global_data_campaign BY (country_id, UPPER(segment_cd)) LEFT OUTER,
    segment_org               BY (seg_gbic_op_id, seg_local_cd);

gbic_global_data_tariff = JOIN
    gbic_global_data_segment BY (country_id, month_id, tariff_plan_id) LEFT OUTER,
    m_tariff_plan            BY (tar_country_id, tar_month_id, tar_tariff_plan_id);

gbic_global_data_prev_tariff = JOIN
    gbic_global_data_tariff BY (country_id, month_id, prev_tariff_plan_id) LEFT OUTER,
    m_tariff_plan           BY (tar_country_id, tar_month_id, tar_tariff_plan_id);

store_data = FOREACH gbic_global_data_prev_tariff GENERATE
    gbic_op_name                                                          AS (gbic_op_name:               chararray),
    customer_id                                                           AS (customer_id:                chararray),
    msisdn_id                                                             AS (msisdn_id:                  chararray),
    subscription_id                                                       AS (subscription_id:            chararray),
    activation_dt                                                         AS (activation_dt:              chararray),
    movement_dt                                                           AS (movement_dt:                chararray),
    movement_id                                                           AS (movement_id:                chararray),
    (mov_movement_des IS NULL? 'UNKNOWN': mov_movement_des)               AS (movement_des:               chararray),
    (mov_count_movement_qt is NULL ? -1 : mov_count_movement_qt)          AS (count_movement_qt:          int),
    (mov_tp_local_group_id IS NULL? 'UNKNOWN': mov_tp_local_group_id)     AS (mov_grp_local_cd:           chararray),
    (mov_tp_local_group_des IS NULL? 'UNKNOWN': mov_tp_local_group_des)   AS (mov_grp_local_name:         chararray),
    (mov_tp_global_group_id IS NULL? -1: mov_tp_global_group_id)          AS (mov_grp_global_id:          int),
    (mov_tp_global_group_des IS NULL? 'UNKNOWN': mov_tp_global_group_des) AS (mov_grp_global_name:        chararray),
    movement_channel_id                                                   AS (movement_channel_id:        chararray),
    campaign_id                                                           AS (campaign_id:                chararray),
    (camp_campaign_des IS NULL? 'UNKNOWN': camp_campaign_des)             AS (campaign_des:               chararray),
    segment_cd                                                            AS (seg_local_cd:               chararray),
    (seg_local_name IS NULL? 'UNKNOWN': seg_local_name)                   AS (seg_local_name:             chararray),
    (seg_global_id IS NULL? -1: seg_global_id)                            AS (seg_global_id:              int),
    (seg_global_name IS NULL? 'UNKNOWN': seg_global_name)                 AS (seg_global_name:            chararray),
    pre_post_id                                                           AS (pre_post_id:                chararray),
    prev_pre_post_id                                                      AS (prev_pre_post_id:           chararray),
    tariff_plan_id                                                        AS (tariff_plan_id:             chararray),
    (gbic_global_data_tariff::m_tariff_plan::tar_des_plan IS NULL?
        'UNKNOWN':
        gbic_global_data_tariff::m_tariff_plan::tar_des_plan)             AS (tariff_plan_des:            chararray),
    prev_tariff_plan_id                                                   AS (prev_tariff_plan_id:        chararray),
    (m_tariff_plan::tar_des_plan IS NULL?
        'UNKNOWN':
        m_tariff_plan::tar_des_plan)                                      AS (prev_tariff_plan_des:       chararray),
    prod_type_cd                                                          AS (prod_type_cd:               chararray),
    port_op_cd                                                            AS (port_op_cd:                 chararray),
    (oper_port_op_des IS NULL? 'UNKNOWN': oper_port_op_des)               AS (port_op_des:                chararray),
    gbic_op_id                                                            AS (gbic_op_id:                 int),
    '2015-01-01'                                                          AS (month:                      chararray);

STORE store_data INTO 'gbic_global_staging.gbic_global_movements'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
