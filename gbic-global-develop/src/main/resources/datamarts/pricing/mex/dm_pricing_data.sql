-------------------------------------------------------------------------------------------
---                          Pricing Dataset Generation                                 ---
-------------------------------------------------------------------------------------------
--- Description: Script to create dataset for the FWD and BWD samples                   ---
---              from m_lines, invoice, traffic voice, traffic sms                      ---
---              and traffic data files                                                 ---
---                                                                                     ---
--- Parameters:                                                                         ---
---      targetOb:  Gbic global identifier of the ob to extract the sample              ---
---      initial_month                                                                  ---
---      final_month                                                                    ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar targetOb=1                                                      --- 
---           --hivevar initial_month=2015-01-10                                        ---
---           --hivevar final_month=2015-10-10                                          ---
---           -f dm_pricing_data.sql                                                    ---
---                                                                                     ---
-------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- set hivevar:targetOb=9;
-- set hivevar:initial_month=2015-11-01;
-- set hivevar:final_month=2015-12-01;
-- set hive.exec.dynamic.partition.mode=nonstrict;
-- set hive.auto.convert.join=false;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
FROM (
    
    SELECT *
    FROM {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_SAMPLE
    WHERE gbic_op_id=${targetOb}
    
) sample

INNER JOIN (
    
    SELECT id_country                                   AS id_country,
           concat(id_month,'-01')                       AS id_month,
           num_msisdn_gbl_ext                           AS num_msisdn_gbl_ext,
           id_customer                                  AS id_customer,
           IF(fl_pre_postpaid=='X','H',fl_pre_postpaid) AS fl_pre_postpaid,
           dt_activation_line                           AS dt_activation_line,
           cod_imei                                     AS cod_imei,
           id_contract                                  AS id_contract,
           des_contract                                 AS des_contract,
           vol_internet                                 AS vol_internet,
           model                                        AS model,
           des_market_categ                             AS des_market_categ,
           des_tef_categ                                AS des_tef_categ,
           fl_line_status                               AS fl_line_status,
           id_business_segment                          AS id_business_segment,
           des_business_segment                         AS des_business_segment,
           id_city                                      AS id_city,
           rev_traffic_data                             AS rev_traffic_data,
           num_call_voice_out                           AS num_call_voice_out,
           num_min_voice_out                            AS num_min_voice_out,
           num_mess_out                                 AS num_mess_out,
           num_top_up                                   AS num_top_up,
           rev_top_up                                   AS rev_top_up,
           rev_tot                                      AS rev_tot,
           sum(rev_quota_data)                          AS quota_data_rv,
           sum(rev_quota_voice)                         AS quota_voice_rv,
           sum(rev_quota_mess)                          AS quota_mess_rv,
           sum(rev_quota_agg)                           AS quota_agg_rv,
           sum(rev_traffic_data)                        AS traffic_data_rv,
           sum(rev_traffic_voice)                       AS traffic_voice_rv,
           sum(rev_traffic_mess)                        AS traffic_mess_rv,
           sum(rev_traffic_agg)                         AS traffic_agg_rv,
           sum(rev_roaming)                             AS roaming_rv,
           sum(rev_sva)                                 AS sva_rv,
           sum(rev_packs)                               AS packs_rv,
           sum(rev_top_up_ex)                           AS top_up_ex_rv,
           sum(rev_top_up_co)                           AS top_up_co_rv,
           sum(rev_gb_camp)                             AS gb_camp_rv,
           sum(rev_others)                              AS others_rv,
           sum(rev_tot)                                 AS tot_rv,
           sum(rev_top_up)                              AS top_up_rv,
           sum(rev_itx)                                 AS itx_rv,
           sum(exp_itx)                                 AS exp_itx_rv,
           sum(rev_tot)                                 AS total_invoice_rv
    FROM LTV.GBIC_GLOBAL_LTV_FULLDET_MX
    WHERE id_country = ${targetOb}
      AND id_business_segment = 4 -- Filtering by segment (only consumer/individuals)
    GROUP BY id_country,
             id_month,
             num_msisdn_gbl_ext,
             id_customer,
             fl_pre_postpaid,
             dt_activation_line,
             cod_imei,
             id_contract,
             des_contract,
             vol_internet,
             model,
             des_market_categ,
             des_tef_categ,
             fl_line_status,
             id_business_segment,
             des_business_segment,
             id_city,
             rev_traffic_data,
             num_call_voice_out,
             num_min_voice_out,
             num_mess_out,
             num_top_up,
             rev_top_up,
             rev_tot
    
) lines
  ON  lines.num_msisdn_gbl_ext = sample.msisdn_id
  AND lines.id_country = sample.gbic_op_id

LEFT OUTER JOIN (
    
    SELECT gbic_op_id,
           id_tariff,
           desc_tariff
    FROM {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_TARIFFS_ORC
    WHERE gbic_op_id = ${targetOb}
    
) tariff
  ON  lines.id_country = tariff.gbic_op_id
  AND lines.id_contract = tariff.id_tariff

INSERT OVERWRITE TABLE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_DATA
PARTITION (gbic_op_id,month,type)
SELECT IF(lines.fl_pre_postpaid == sample.pre_post_id, 0, 1)              AS change,
       'MOVISTAR MEXICO',
       'MXN',
       lines.num_msisdn_gbl_ext,
       '-1',
       'UNKNOWN',
       lines.id_customer,
       lines.id_customer,
       lines.id_customer,
       lines.dt_activation_line,
       'UNKNOWN',
       lines.cod_imei,
       substr(lines.cod_imei, 1, 8)                                       AS tac_id,
       'UNKNOWN',
       lines.model,
       lines.des_market_categ,
       lines.des_tef_categ,
       'UNKNOWN',
       'UNKNOWN',
       'UNKNOWN',
       lines.fl_line_status,
       lines.id_business_segment,
       lines.des_business_segment,
       lines.id_business_segment,
       IF(lines.des_business_segment=='Particular',
          'Consumer',
          lines.des_business_segment)                                     AS seg_global_name,
       fl_pre_postpaid                                                    AS fl_pre_postpaid,
       -1,
       lines.id_contract                                                  AS tariff_plan_id,
       tariff.desc_tariff                                                 AS tariff_plan_des,
       lines.id_city,
       0,
       -1,
       -1,
       0,
       lines.rev_traffic_data,
       0,
       0,
       0,
       0,
       lines.vol_internet,
       0,
       lines.num_call_voice_out,
       lines.num_min_voice_out,
       lines.num_mess_out,
       -1,
       lines.num_top_up,
       lines.rev_top_up,
       0,
       0,
       IF (lines.id_country IS NULL,0.0,lines.rev_tot-lines.rev_top_up)   AS no_top_up_rv,
       lines.rev_tot                                                      AS total_rv,
       0                                                                  AS bta_ind,
       IF (lines.id_country IS NULL,0.0,lines.quota_data_rv)              AS quota_data_rv,
       IF (lines.id_country IS NULL,0.0,lines.quota_voice_rv)             AS quota_voice_rv,
       IF (lines.id_country IS NULL,0.0,lines.quota_mess_rv)              AS quota_mess_rv,
       IF (lines.id_country IS NULL,0.0,lines.quota_agg_rv)               AS quota_agg_rv,
       IF (lines.id_country IS NULL,0.0,lines.traffic_data_rv)            AS traffic_data_rv,
       IF (lines.id_country IS NULL,0.0,lines.traffic_voice_rv)           AS traffic_voice_rv,
       IF (lines.id_country IS NULL,0.0,lines.traffic_mess_rv)            AS traffic_mess_rv,
       IF (lines.id_country IS NULL,0.0,lines.traffic_agg_rv)             AS traffic_agg_rv,
       IF (lines.id_country IS NULL,0.0,lines.roaming_rv)                 AS roaming_rv,
       IF (lines.id_country IS NULL,0.0,lines.sva_rv)                     AS sva_rv,
       IF (lines.id_country IS NULL,0.0,lines.packs_rv)                   AS packs_rv,
       IF (lines.id_country IS NULL,0.0,lines.top_up_ex_rv)               AS top_up_ex_rv,
       IF (lines.id_country IS NULL,0.0,lines.top_up_co_rv)               AS top_up_co_rv,
       IF (lines.id_country IS NULL,0.0,lines.gb_camp_rv)                 AS gb_camp_rv,
       IF (lines.id_country IS NULL,0.0,lines.others_rv)                  AS others_rv,
       IF (lines.id_country IS NULL,0.0,lines.rev_tot-lines.top_up_co_rv) AS tot_rv,
       IF (lines.id_country IS NULL,0.0,lines.top_up_rv)                  AS top_up_rv,
       IF (lines.id_country IS NULL,0.0,lines.itx_rv)                     AS itx_rv,
       IF (lines.id_country IS NULL,0.0,lines.exp_itx_rv)                 AS exp_itx_rv,
       IF (lines.id_country IS NULL,0.0,lines.total_invoice_rv)           AS total_invoice_rv,
       '---'                                                              AS billing_cycle_id,
       0.0                                                                AS fixed_out,
       0.0                                                                AS min_offnet_mobile_out,
       0.0                                                                AS min_onnet_mobile_out,
       0.0                                                                AS free_and_rcm_out,
       0.0                                                                AS other_out,
       0.0                                                                AS out_national_onnet_rv,
       0.0                                                                AS out_national_offnet_rv,
       0.0                                                                AS out_national_fixed_rv,
       0.0                                                                AS other_out_rv,
       0.0                                                                AS min_out_bundled,
       0.0                                                                AS min_out_not_bundled,
       0.0                                                                AS min_out_exceed,
       0.0                                                                AS min_2g_out,
       0.0                                                                AS min_3g_out,
       0.0                                                                AS min_4g_out,
       0                                                                  AS sms_offnet_out_qt,
       0                                                                  AS sms_onnet_out_qt,
       0                                                                  AS sms_international_out_qt,
       0                                                                  AS sms_roaming_out_qt,
       0.0                                                                AS sms_out_bundled_rv,
       0.0                                                                AS sms_out_not_bundled_rv,
       0.0                                                                AS sms_roaming_out_rv,
       0.0                                                                AS total_qt,
       0.0                                                                AS mb_2g_qt,
       0.0                                                                AS mb_3g_qt,
       0.0                                                                AS mb_4g_qt,
       0.0                                                                AS mb_roaming,
       lines.id_country                                                   AS gbic_op_id,
       lines.id_month                                                     AS month,
       sample.type                                                        AS type
;
