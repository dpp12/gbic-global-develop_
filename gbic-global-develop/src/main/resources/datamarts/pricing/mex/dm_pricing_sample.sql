------------------------------------------------------------------------------------------------------
---                          Pricing Sample Generation                                             ---
------------------------------------------------------------------------------------------------------
--- Description: Script to extract a sample of consumer lines from LTV.GBIC_GLOBAL_LTV_FULLDET_MX  ---
---              for an specific ob                                                                ---
--- Parameters:                                                                                    ---
---      targetOb:  Gbic global identifier of the ob to extract the sampel                         ---
---      sampleType: 'FWD' to extract the sample from the older id_month of every ob               ---
---                  'BWD' to extract the sample from the newer id_month of every ob               ---
---      pLimit:     Number of Prepaid lines for the sample                                        ---
---      cLimit:     Number of Contract lines for the sample                                       ---
---      hLimit:     Number of Hybrid lines for the sample                                         ---
---                                                                                                ---
--- Execution example:                                                                             ---
---      hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict                                ---
---           --hivevar targetOb=1                                                                 ---
---           --hivevar sampleType='BWD'                                                           ---
---           --hivevar pLimit=100000                                                              ---
---           --hivevar cLimit=25000                                                               ---
---           --hivevar hLimit=25000                                                               ---
---           -f dm_pricing_sample.sql                                                             ---
---                                                                                                ---
------------------------------------------------------------------------------------------------------

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- set hivevar:targetOb=9;
-- set hivevar:sampleType=BWD;
-- set hivevar:pLimit=100000; 
-- set hivevar:cLimit=25000;
-- set hivevar:hLimit=25000;
-- set hive.exec.dynamic.partition.mode=nonstrict;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
USE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING;
ALTER TABLE GBIC_GLOBAL_DM_PRICING_SAMPLE DROP IF EXISTS PARTITION ( type='${sampleType}', gbic_op_id=${targetOb} );

FROM
    (SELECT
        *
    FROM  
        (SELECT 
            dataset.*,
            -- rank() used instead of row_number() due to a bug in hive 0.13.
            -- Using rank requires ordering by num_msisdn_gbl besides r_index
            rank() OVER (PARTITION BY 
                            dataset.id_country,dataset.id_month,dataset.fl_pre_postpaid 
                         ORDER BY 
                            dataset.r_index,dataset.num_msisdn_gbl) AS ranking
        FROM
            (SELECT 
                id_country,
                id_month,
                fl_pre_postpaid,
                num_msisdn_gbl,
                id_customer,
                rand() AS r_index
             FROM
                LTV.GBIC_GLOBAL_LTV_FULLDET_MX
             WHERE
                id_country=${targetOb} AND 
                id_business_segment=4 -- Filtering by segment (only consumer/individuals)
            ) dataset     
            INNER JOIN
            (SELECT
                id_country,
                IF('${sampleType}'='FWD',min(id_month),max(id_month)) AS id_month
            FROM 
                LTV.GBIC_GLOBAL_LTV_FULLDET_MX
            WHERE
                id_country=${targetOb}
            GROUP BY 
                id_country
            )m
            ON
                dataset.id_country=m.id_country AND
                dataset.id_month=m.id_month
        ) rk
    WHERE --This where clause must be included here.In the insert causes vertex error
        (fl_pre_postpaid = 'P' AND ranking <= ${pLimit}) OR
        (fl_pre_postpaid = 'C' AND ranking <= ${cLimit}) OR
        (fl_pre_postpaid = 'X' AND ranking <= ${hLimit}) 
    )filter
    
INSERT OVERWRITE TABLE {{ project.prefix }}GBIC_GLOBAL_DM_PRICING.GBIC_GLOBAL_DM_PRICING_SAMPLE
PARTITION (type,gbic_op_id,month,pre_post_id)
SELECT
    num_msisdn_gbl,
    id_customer,
    '-1',
    '${sampleType}'                              AS type,
    id_country                                   AS gbic_op_id,
    concat(id_month,'-01')                       AS month,
    IF(fl_pre_postpaid=='X','H',fl_pre_postpaid) AS pre_post_id;
