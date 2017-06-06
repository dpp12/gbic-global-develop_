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
-- set hivevar:targetOb=1;
-- set hivevar:initial_month=2015-10-01;
-- set hivevar:final_month=2015-10-01;
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
    
    SELECT *
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_M_LINES
    WHERE gbic_op_id=${targetOb}
      AND seg_global_id = 6 -- Filtering by segment (only consumer)
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
    
) lines
  ON  lines.gbic_op_id      = sample.gbic_op_id
  AND lines.msisdn_id       = sample.msisdn_id
  AND lines.subscription_id = sample.subscription_id

LEFT OUTER JOIN (
    
    SELECT gbic_op_id,
           month,
           msisdn_id,
           customer_id,
           sum(quota_data_rv)    AS quota_data_rv,
           sum(quota_voice_rv)   AS quota_voice_rv,
           sum(quota_mess_rv)    AS quota_mess_rv,
           sum(quota_agg_rv)     AS quota_agg_rv,
           sum(traffic_data_rv)  AS traffic_data_rv,
           sum(traffic_voice_rv) AS traffic_voice_rv,
           sum(traffic_mess_rv)  AS traffic_mess_rv,
           sum(traffic_agg_rv)   AS traffic_agg_rv,
           sum(roaming_rv)       AS roaming_rv,
           sum(sva_rv)           AS sva_rv,
           sum(packs_rv)         AS packs_rv,
           sum(top_up_ex_rv)     AS top_up_ex_rv,
           sum(top_up_co_rv)     AS top_up_co_rv,
           sum(gb_camp_rv)       AS gb_camp_rv,
           sum(others_rv)        AS others_rv,
           sum(tot_rv)           AS tot_rv,
           sum(top_up_rv)        AS top_up_rv,
           sum(itx_rv)           AS itx_rv,
           sum(exp_itx_rv)       AS exp_itx_rv,
           sum(total_invoice_rv) AS total_invoice_rv
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_INVOICE
    WHERE gbic_op_id = ${targetOb}
    GROUP BY gbic_op_id,
             month,
             msisdn_id,
             customer_id
    
) invoice
  ON  lines.gbic_op_id = invoice.gbic_op_id
  AND lines.month      = invoice.month
  AND lines.msisdn_id  = invoice.msisdn_id

LEFT JOIN (
    
    SELECT subscription_id,
           msisdn_id,
           gbic_op_id,
           month,
           SUM(min_offnet_fixed_out)       AS min_offnet_fixed_out,
           SUM(min_onnet_fixed_out)        AS min_onnet_fixed_out,
           SUM(min_offnet_mobile_out)      AS min_offnet_mobile_out,
           SUM(min_onnet_mobile_out)       AS min_onnet_mobile_out,
           SUM(min_onnet_free_out)         AS min_onnet_free_out,
           SUM(min_onnet_rcm_out)          AS min_onnet_rcm_out,
           SUM(min_international_out)      AS min_international_out,
           SUM(min_out_special_numbers)    AS min_out_special_numbers,
           SUM(min_roaming_out)            AS min_roaming_out,
           SUM(out_national_onnet_rv)      AS out_national_onnet_rv,
           SUM(out_national_offnet_rv)     AS out_national_offnet_rv,
           SUM(out_national_fixed_rv)      AS out_national_fixed_rv,
           SUM(out_international_rv)       AS out_international_rv,
           SUM(roaming_rv)                 AS roaming_rv,
           SUM(out_other_rv)               AS out_other_rv,
           SUM(min_fixed_out_bundled)      AS min_fixed_out_bundled,
           SUM(min_mobile_out_bundled)     AS min_mobile_out_bundled,
           SUM(min_fixed_out_not_bundled)  AS min_fixed_out_not_bundled,
           SUM(min_mobile_out_not_bundled) AS min_mobile_out_not_bundled,
           SUM(min_fixed_out_exceed)       AS min_fixed_out_exceed,
           SUM(min_mobile_out_exceed)      AS min_mobile_out_exceed,
           SUM(min_2g_out)                 AS min_2g_out,
           SUM(min_3g_out)                 AS min_3g_out,
           SUM(min_4g_out)                 AS min_4g_out
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TRAFFIC_VOICE
    WHERE gbic_op_id = ${targetOb}
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
    GROUP BY subscription_id,
             msisdn_id,
             gbic_op_id,
             month
    
) tv
  ON  lines.gbic_op_id       = tv.gbic_op_id
  AND lines.month            = tv.month
  AND lines.msisdn_id        = tv.msisdn_id
  AND lines.subscription_id  = tv.subscription_id

-----------------------------------------------------------------------------------------------------------
-- This join is only for ARG, it solves a problem when msisdn_id isn't informed ---------------------------
-- We have to ADD results with msisdn_id = '5gFf+/QdWlSpOmpLUxSDswzAsEFl3JKFCZ9O9JvC1m0=' -----------------
-- This msisdn_id is "-5" encripted. We are receiving it when its not informed on traffic_xxx tables ------
-----------------------------------------------------------------------------------------------------------
LEFT JOIN (
    
    SELECT subscription_id,
           msisdn_id,
           gbic_op_id,
           month,
           SUM(min_offnet_fixed_out)       AS min_offnet_fixed_out,
           SUM(min_onnet_fixed_out)        AS min_onnet_fixed_out,
           SUM(min_offnet_mobile_out)      AS min_offnet_mobile_out,
           SUM(min_onnet_mobile_out)       AS min_onnet_mobile_out,
           SUM(min_onnet_free_out)         AS min_onnet_free_out,
           SUM(min_onnet_rcm_out)          AS min_onnet_rcm_out,
           SUM(min_international_out)      AS min_international_out,
           SUM(min_out_special_numbers)    AS min_out_special_numbers,
           SUM(min_roaming_out)            AS min_roaming_out,
           SUM(out_national_onnet_rv)      AS out_national_onnet_rv,
           SUM(out_national_offnet_rv)     AS out_national_offnet_rv,
           SUM(out_national_fixed_rv)      AS out_national_fixed_rv,
           SUM(out_international_rv)       AS out_international_rv,
           SUM(roaming_rv)                 AS roaming_rv,
           SUM(out_other_rv)               AS out_other_rv,
           SUM(min_fixed_out_bundled)      AS min_fixed_out_bundled,
           SUM(min_mobile_out_bundled)     AS min_mobile_out_bundled,
           SUM(min_fixed_out_not_bundled)  AS min_fixed_out_not_bundled,
           SUM(min_mobile_out_not_bundled) AS min_mobile_out_not_bundled,
           SUM(min_fixed_out_exceed)       AS min_fixed_out_exceed,
           SUM(min_mobile_out_exceed)      AS min_mobile_out_exceed,
           SUM(min_2g_out)                 AS min_2g_out,
           SUM(min_3g_out)                 AS min_3g_out,
           SUM(min_4g_out)                 AS min_4g_out
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TRAFFIC_VOICE
    WHERE gbic_op_id = 2
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
      AND msisdn_id = '5gFf+/QdWlSpOmpLUxSDswzAsEFl3JKFCZ9O9JvC1m0='
    GROUP BY subscription_id,
             msisdn_id,
             gbic_op_id,
             month
    
) tv_noMsisdn
  ON  lines.gbic_op_id       = tv_noMsisdn.gbic_op_id
  AND lines.month            = tv_noMsisdn.month
  AND lines.subscription_id  = tv_noMsisdn.subscription_id

LEFT JOIN (
    
    SELECT subscription_id,
           msisdn_id,
           gbic_op_id,
           month,
           SUM(sms_offnet_out_qt)        AS sms_offnet_out_qt,
           SUM(sms_onnet_out_qt)         AS sms_onnet_out_qt,
           SUM(sms_international_out_qt) AS sms_international_out_qt,
           SUM(sms_roaming_out_qt)       AS sms_roaming_out_qt,
           SUM(sms_out_bundled_rv)       AS sms_out_bundled_rv,
           SUM(sms_out_not_bundled_rv)   AS sms_out_not_bundled_rv,
           SUM(sms_roaming_out_rv)       AS sms_roaming_out_rv
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TRAFFIC_SMS
    WHERE gbic_op_id = ${targetOb}
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
    GROUP BY subscription_id,
             msisdn_id,
             gbic_op_id,
             month
    
) ts
  ON  lines.gbic_op_id       = ts.gbic_op_id
  AND lines.month            = ts.month
  AND lines.msisdn_id        = ts.msisdn_id
  AND lines.subscription_id  = ts.subscription_id

-----------------------------------------------------------------------------------------------------------
-- This join is only for ARG, it solves a problem when msisdn_id isn't informed ---------------------------
-- We have to ADD results with msisdn_id = '5gFf+/QdWlSpOmpLUxSDswzAsEFl3JKFCZ9O9JvC1m0=' -----------------
-- This msisdn_id is "-5" encripted. We are receiving it when its not informed on traffic_xxx tables ------
-----------------------------------------------------------------------------------------------------------
LEFT JOIN (
    
    SELECT subscription_id,
           msisdn_id,
           gbic_op_id,
           month,
           SUM(sms_offnet_out_qt)        AS sms_offnet_out_qt,
           SUM(sms_onnet_out_qt)         AS sms_onnet_out_qt,
           SUM(sms_international_out_qt) AS sms_international_out_qt,
           SUM(sms_roaming_out_qt)       AS sms_roaming_out_qt,
           SUM(sms_out_bundled_rv)       AS sms_out_bundled_rv,
           SUM(sms_out_not_bundled_rv)   AS sms_out_not_bundled_rv,
           SUM(sms_roaming_out_rv)       AS sms_roaming_out_rv
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TRAFFIC_SMS
    WHERE gbic_op_id = 2
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
      AND msisdn_id = '5gFf+/QdWlSpOmpLUxSDswzAsEFl3JKFCZ9O9JvC1m0='
    GROUP BY subscription_id,
             msisdn_id,
             gbic_op_id,
             month
    
) ts_noMsisdn
  ON  lines.gbic_op_id       = ts_noMsisdn.gbic_op_id
  AND lines.month            = ts_noMsisdn.month
  AND lines.subscription_id  = ts_noMsisdn.subscription_id

LEFT JOIN (
    
    SELECT subscription_id,
           msisdn_id,
           gbic_op_id,
           month,
           SUM(total_qt)   AS total_qt,
           SUM(mb_2g_qt)   AS mb_2g_qt,
           SUM(mb_3g_qt)   AS mb_3g_qt,
           SUM(mb_4g_qt)   AS mb_4g_qt,
           SUM(mb_roaming) AS mb_roaming
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TRAFFIC_DATA
    WHERE gbic_op_id = ${targetOb}
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
    GROUP BY subscription_id,
             msisdn_id,
             gbic_op_id,
             month
    
) td
  ON  lines.gbic_op_id       = td.gbic_op_id
  AND lines.month            = td.month
  AND lines.msisdn_id        = td.msisdn_id
  AND lines.subscription_id  = td.subscription_id

-----------------------------------------------------------------------------------------------------------
-- This join is only for ARG, it solves a problem when msisdn_id isn't informed ---------------------------
-- We have to ADD results with msisdn_id = '5gFf+/QdWlSpOmpLUxSDswzAsEFl3JKFCZ9O9JvC1m0=' -----------------
-- This msisdn_id is "-5" encripted. We are receiving it when its not informed on traffic_xxx tables ------
-----------------------------------------------------------------------------------------------------------
LEFT JOIN (
    
    SELECT subscription_id,
           msisdn_id,
           gbic_op_id,
           month,
           SUM(total_qt)   AS total_qt,
           SUM(mb_2g_qt)   AS mb_2g_qt,
           SUM(mb_3g_qt)   AS mb_3g_qt,
           SUM(mb_4g_qt)   AS mb_4g_qt,
           SUM(mb_roaming) AS mb_roaming
    FROM {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TRAFFIC_DATA
    WHERE gbic_op_id = 2
      AND month >= '${initial_month}'
      AND month <= '${final_month}'
      AND msisdn_id = '5gFf+/QdWlSpOmpLUxSDswzAsEFl3JKFCZ9O9JvC1m0='
    GROUP BY subscription_id,
             msisdn_id,
             gbic_op_id,
             month
    
) td_noMsisdn
  ON  lines.gbic_op_id       = td_noMsisdn.gbic_op_id
  AND lines.month            = td_noMsisdn.month
  AND lines.subscription_id  = td_noMsisdn.subscription_id

INSERT OVERWRITE TABLE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_DATA
PARTITION (gbic_op_id, month, type)
SELECT IF(lines.pre_post_id == sample.pre_post_id, 0, 1)                                      AS change,
       lines.gbic_op_name,
       lines.currency,
       lines.msisdn_id,
       lines.subscription_id,
       lines.imsi_id,
       lines.customer_id,
       lines.mobile_customer_id,
       lines.party_type_cd,
       lines.activation_dt,
       lines.prod_type_cd,
       lines.imei_num,
       lines.tac_id,
       lines.des_manufact,
       lines.des_model,
       lines.market_category,
       lines.tef_category,
       lines.os,
       lines.version_os,
       lines.technology,
       lines.line_status_cd,
       lines.seg_local_cd,
       lines.seg_local_name,
       lines.seg_global_id,
       lines.seg_global_name,
       lines.pre_post_id,
       lines.account_id,
       lines.tariff_plan_id,
       lines.tariff_plan_des,
       lines.postal_cd,
       lines.multisim_ind,
       lines.exceed_ind,
       lines.data_tariff_ind,
       lines.extra_data_num,
       lines.extra_data_rv,
       lines.extra_data_qt,
       lines.ppu_num,
       lines.ppu_rv,
       lines.ppu_qt,
       lines.data_consumed_qt,
       lines.data_bundled_qt,
       lines.call_voice_qt,
       lines.voice_consumed_qt,
       lines.sms_consumed_qt,
       lines.prepaid_top_up_id,
       lines.top_up_cost_num,
       lines.top_up_cost_rv,
       lines.top_up_promo_num,
       lines.top_up_promo_rv,
       lines.no_top_up_rv,
       lines.total_rv,
       lines.bta_ind,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.quota_data_rv)                              AS quota_data_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.quota_voice_rv)                             AS quota_voice_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.quota_mess_rv)                              AS quota_mess_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.quota_agg_rv)                               AS quota_agg_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.traffic_data_rv)                            AS traffic_data_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.traffic_voice_rv)                           AS traffic_voice_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.traffic_mess_rv)                            AS traffic_mess_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.traffic_agg_rv)                             AS traffic_agg_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.roaming_rv)                                 AS roaming_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.sva_rv)                                     AS sva_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.packs_rv)                                   AS packs_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.top_up_ex_rv)                               AS top_up_ex_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.top_up_co_rv)                               AS top_up_co_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.gb_camp_rv)                                 AS gb_camp_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.others_rv)                                  AS others_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.tot_rv)                                     AS tot_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.top_up_rv)                                  AS top_up_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.itx_rv)                                     AS itx_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.exp_itx_rv)                                 AS exp_itx_rv,
       IF (invoice.gbic_op_id IS NULL,0.0,invoice.total_invoice_rv)                           AS total_invoice_rv,
       lines.billing_cycle_id,
       IF ((tv.min_offnet_fixed_out + tv.min_onnet_fixed_out) IS NULL,
           0.0,
           (tv.min_offnet_fixed_out + tv.min_onnet_fixed_out))
          +IF ((tv_noMsisdn.min_offnet_fixed_out + tv_noMsisdn.min_onnet_fixed_out) IS NULL,
          0.0,(tv_noMsisdn.min_offnet_fixed_out + tv_noMsisdn.min_onnet_fixed_out))                                           AS fixed_out,
       IF (tv.min_offnet_mobile_out IS NULL,0.0,tv.min_offnet_mobile_out)
          +IF ((tv_noMsisdn.min_offnet_mobile_out) IS NULL,0.0,(tv_noMsisdn.min_offnet_mobile_out)),
       IF (tv.min_onnet_mobile_out IS NULL,0.0,tv.min_onnet_mobile_out)
          +IF ((tv_noMsisdn.min_onnet_mobile_out) IS NULL,0.0,(tv_noMsisdn.min_onnet_mobile_out)),
       IF ((tv.min_onnet_free_out + tv.min_onnet_rcm_out) IS NULL,0.0,(tv.min_onnet_free_out + tv.min_onnet_rcm_out))
          +IF ((tv_noMsisdn.min_onnet_free_out + tv_noMsisdn.min_onnet_rcm_out) IS NULL,
          0.0,(tv_noMsisdn.min_onnet_free_out + tv_noMsisdn.min_onnet_rcm_out))                                               AS free_and_rcm_out,
       IF ((tv.min_international_out + tv.min_out_special_numbers + tv.min_roaming_out) IS NULL,
            0.0,
            (tv.min_international_out + tv.min_out_special_numbers + tv.min_roaming_out))
          +IF ((tv_noMsisdn.min_international_out + tv_noMsisdn.min_out_special_numbers + tv_noMsisdn.min_roaming_out) IS NULL,
          0.0,(tv_noMsisdn.min_international_out + tv_noMsisdn.min_out_special_numbers + tv_noMsisdn.min_roaming_out))        AS other_out,
       IF (tv.out_national_onnet_rv IS NULL,0.0,tv.out_national_onnet_rv)
          +IF ((tv_noMsisdn.out_national_onnet_rv) IS NULL,0.0,(tv_noMsisdn.out_national_onnet_rv)),
       IF (tv.out_national_offnet_rv IS NULL,0.0,tv.out_national_offnet_rv)
          +IF ((tv_noMsisdn.out_national_offnet_rv) IS NULL,0.0,(tv_noMsisdn.out_national_offnet_rv)),
       IF (tv.out_national_fixed_rv IS NULL,0.0,tv.out_national_fixed_rv)
          +IF ((tv_noMsisdn.out_national_fixed_rv) IS NULL,0.0,(tv_noMsisdn.out_national_fixed_rv)),
       IF ((tv.out_international_rv + tv.roaming_rv + tv.out_other_rv) IS NULL,
           0.0,
           (tv.out_international_rv + tv.roaming_rv + tv.out_other_rv))
          +IF ((tv_noMsisdn.out_international_rv + tv_noMsisdn.roaming_rv + tv_noMsisdn.out_other_rv) IS NULL,
          0.0,(tv_noMsisdn.out_international_rv + tv_noMsisdn.roaming_rv + tv_noMsisdn.out_other_rv))                        AS other_out_rv,
       IF ((tv.min_fixed_out_bundled + tv.min_mobile_out_bundled) IS NULL,
           0.0,
           (tv.min_fixed_out_bundled + tv.min_mobile_out_bundled))
          +IF ((tv_noMsisdn.min_fixed_out_bundled + tv_noMsisdn.min_mobile_out_bundled) IS NULL,
          0.0,(tv_noMsisdn.min_fixed_out_bundled + tv_noMsisdn.min_mobile_out_bundled))                                      AS min_out_bundled,
       IF ((tv.min_fixed_out_not_bundled + tv.min_mobile_out_not_bundled) IS NULL,
           0.0,
           (tv.min_fixed_out_not_bundled + tv.min_mobile_out_not_bundled))
          +IF ((tv_noMsisdn.min_fixed_out_not_bundled + tv_noMsisdn.min_mobile_out_not_bundled) IS NULL,
          0.0,(tv_noMsisdn.min_fixed_out_not_bundled + tv_noMsisdn.min_mobile_out_not_bundled))                              AS min_out_not_bundled,
       IF ((tv.min_fixed_out_exceed + tv.min_mobile_out_exceed) IS NULL,
           0.0,
           (tv.min_fixed_out_exceed + tv.min_mobile_out_exceed))
          +IF ((tv_noMsisdn.min_fixed_out_exceed + tv_noMsisdn.min_mobile_out_exceed) IS NULL,
          0.0,(tv_noMsisdn.min_fixed_out_exceed + tv_noMsisdn.min_mobile_out_exceed))                                        AS min_out_exceed,
       IF (tv.min_2g_out IS NULL,0.0,tv.min_2g_out)
          +IF ((tv_noMsisdn.min_2g_out) IS NULL,0.0,(tv_noMsisdn.min_2g_out)),
       IF (tv.min_3g_out IS NULL,0.0,tv.min_3g_out)
          +IF ((tv_noMsisdn.min_3g_out) IS NULL,0.0,(tv_noMsisdn.min_3g_out)),
       IF (tv.min_4g_out IS NULL,0.0,tv.min_4g_out)
          +IF ((tv_noMsisdn.min_4g_out) IS NULL,0.0,(tv_noMsisdn.min_4g_out)),
       IF (ts.sms_offnet_out_qt IS NULL,0.0,ts.sms_offnet_out_qt)
          +IF (ts_noMsisdn.sms_offnet_out_qt IS NULL,0.0,ts_noMsisdn.sms_offnet_out_qt),
       IF (ts.sms_onnet_out_qt IS NULL,0.0,ts.sms_onnet_out_qt)
          +IF (ts_noMsisdn.sms_onnet_out_qt IS NULL,0.0,ts_noMsisdn.sms_onnet_out_qt),
       IF (ts.sms_international_out_qt IS NULL,0.0,ts.sms_international_out_qt)
          +IF (ts_noMsisdn.sms_international_out_qt IS NULL,0.0,ts_noMsisdn.sms_international_out_qt),
       IF (ts.sms_roaming_out_qt IS NULL,0.0,ts.sms_roaming_out_qt)
          +IF ((ts_noMsisdn.sms_roaming_out_qt) IS NULL,0.0,(ts_noMsisdn.sms_roaming_out_qt)),
       IF (ts.sms_out_bundled_rv IS NULL,0.0,ts.sms_out_bundled_rv)
          +IF ((ts_noMsisdn.sms_out_bundled_rv) IS NULL,0.0,(ts_noMsisdn.sms_out_bundled_rv)),
       IF (ts.sms_out_not_bundled_rv IS NULL,0.0,ts.sms_out_not_bundled_rv)
          +IF ((ts_noMsisdn.sms_out_not_bundled_rv) IS NULL,0.0,(ts_noMsisdn.sms_out_not_bundled_rv)),
       IF (ts.sms_roaming_out_rv IS NULL,0.0,ts.sms_roaming_out_rv)
          +IF ((ts_noMsisdn.sms_roaming_out_rv) IS NULL,0.0,(ts_noMsisdn.sms_roaming_out_rv)),
       IF (td.total_qt IS NULL,0.0,td.total_qt)
          +IF ((td_noMsisdn.total_qt) IS NULL,0.0,(td_noMsisdn.total_qt)),
       IF (td.mb_2g_qt IS NULL,0.0,td.mb_2g_qt)
          +IF ((td_noMsisdn.mb_2g_qt) IS NULL,0.0,(td_noMsisdn.mb_2g_qt)),
       IF (td.mb_3g_qt IS NULL,0.0,td.mb_3g_qt)
          +IF ((td_noMsisdn.mb_3g_qt) IS NULL,0.0,(td_noMsisdn.mb_3g_qt)),
       IF (td.mb_4g_qt IS NULL,0.0,td.mb_4g_qt)
          +IF ((td_noMsisdn.mb_4g_qt) IS NULL,0.0,(td_noMsisdn.mb_4g_qt)),
       IF (td.mb_roaming IS NULL,0.0,td.mb_roaming)
          +IF ((td_noMsisdn.mb_roaming) IS NULL,0.0,(td_noMsisdn.mb_roaming)),
       lines.gbic_op_id,
       lines.month,
       sample.type
;
