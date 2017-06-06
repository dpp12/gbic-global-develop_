-------------------------------------------------------------------------------------------
---                          Pricing Dataset Generation                                 ---
-------------------------------------------------------------------------------------------
--- Description: Script to create dataset from daily traffic file                       ---
---                                                                                     ---
--- Parameters:                                                                         ---
---      targetOb:  Gbic global identifier of the ob to extract the sampel              ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar targetOb=1                                                      ---
---           -f dm_pricing_data_daily_traffic.sql                                      ---
---                                                                                     ---
-------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- set hivevar:targetOb = 1;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
FROM
    (SELECT
        *
     FROM
        {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_DAILY_TRAFFIC
     WHERE
        gbic_op_id=${targetOb} AND
        month >= '${initial_month}' AND
        month <= '${final_month}'
    ) dt

    LEFT SEMI JOIN
    (SELECT
        *
     FROM
        {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_SAMPLE
     WHERE
        gbic_op_id=${targetOb} AND
        type='BWD'
    ) sample
    ON
        dt.gbic_op_id     =sample.gbic_op_id      AND
        dt.month          =sample.month           AND
        dt.subscription_id=sample.subscription_id AND
        dt.msisdn_id      =sample.msisdn_id

INSERT OVERWRITE TABLE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_DATA_DAILY_TRAFFIC
PARTITION (gbic_op_id,month)
SELECT
    dt.*
    ;
