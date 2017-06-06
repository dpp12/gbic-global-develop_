-------------------------------------------------------------------------------------------
---                          Pricing Sample Generation                                  ---
-------------------------------------------------------------------------------------------
--- Description: Script to extract a sample of consumer lines from GBIC_GLOBAL_M_LINES  ---
---              for an specific ob                                                     ---
--- Parameters:                                                                         ---
---      targetOb:  Gbic global identifier of the ob to extract the sampel              ---
---      sampleType: 'FWD' to extract the sample from the older month of every ob       ---
---                  'BWD' to extract the sample from the newer month of every ob       ---
---      pLimit:     Number of Prepaid lines for the sample                             ---
---      cLimit:     Number of Contract lines for the sample                            ---
---      hLimit:     Number of Hybrid lines for the sample                              ---
---                                                                                     ---
--- Execution example:                                                                  ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                     ---
---           --hivevar targetOb=1                                                      ---
---           --hivevar sampleType='BWD'                                                ---
---           --hivevar pLimit=100000                                                   ---
---           --hivevar cLimit=25000                                                    ---
---           --hivevar hLimit=25000                                                    ---
---           -f dm_pricing_sample.sql                                                  ---
---                                                                                     ---
-------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- set hivevar:targetOb = 1;
-- set hivevar:sampleType = 'FWD';
-- set hivevar:pLimit = 100000; 
-- set hivevar:cLimit = 25000;
-- set hivevar:hLimit = 25000;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
USE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING;
ALTER TABLE GBIC_GLOBAL_DM_PRICING_SAMPLE DROP IF EXISTS PARTITION (type='${sampleType}',gbic_op_id=${targetOb});

FROM
    (SELECT
        *
    FROM  
        (SELECT 
            dataset.*,
            -- rank() used instead of row_number() due to a bug in hive 0.13.
            -- Using rank requires ordering by msisdn_id besides r_index
            rank() OVER (PARTITION BY 
                            dataset.gbic_op_id,dataset.month,dataset.pre_post_id 
                         ORDER BY 
                            dataset.r_index,dataset.msisdn_id) AS ranking
        FROM
            (SELECT 
                gbic_op_id,
                month,
                pre_post_id,
                msisdn_id,
                subscription_id,
                customer_id,
                rand() AS r_index
             FROM
                {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_M_LINES
             WHERE
                gbic_op_id=${targetOb} AND 
                seg_global_id=6 -- Filtering by segment (only consumer)
            ) dataset     
            INNER JOIN
            (SELECT
                gbic_op_id,
                IF('${sampleType}'='FWD',min(month),max(month)) AS month
            FROM 
                {{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_M_LINES
            WHERE
                gbic_op_id=${targetOb}
            GROUP BY 
                gbic_op_id
            )m
            ON
                dataset.gbic_op_id=m.gbic_op_id AND
                dataset.month=m.month
        ) rk
    WHERE --This where clause must be included here.In the insert causes vertex error
        (pre_post_id = 'P' AND ranking <= ${pLimit}) OR
        (pre_post_id = 'C' AND ranking <= ${cLimit}) OR
        (pre_post_id = 'H' AND ranking <= ${hLimit}) 
    )filter
    
INSERT OVERWRITE TABLE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_SAMPLE
PARTITION (type,gbic_op_id,month,pre_post_id)
SELECT
    msisdn_id,
    customer_id,
    subscription_id,
    '${sampleType}' AS type,
    gbic_op_id,
    month,
    pre_post_id;
