USE {{ project.prefix }}gbic_global_bnss;

INSERT OVERWRITE TABLE report_b2c_bta
PARTITION (gbic_op_id_pt, month_pt)
SELECT
    kpis_agg.gbic_op_id_pt                                                     AS gbic_op_id,
    kpis_agg.month_pt                                                          AS month,
    kpis_agg.bta_ind,
    kpis_agg.loc_lev_7,
    kpis_agg.prod_type_cd,
    IF(kpis_agg.gbic_tariff_id IS NULL, -1, kpis_agg.gbic_tariff_id)           AS gbic_tariff_id,
    CASE -- tariff
        WHEN tarif.is_data_tariff IS NULL THEN 'SIN CRUCE'
        WHEN tarif.is_data_tariff=0       THEN 'SIN DATOS'
        WHEN tarif.is_data_tariff=1       THEN 'CON DATOS'
    END                                                                        AS is_data_tariff,
    IF(tarif.des_plan IS NULL, 'SIN CRUCE', tarif.des_plan)                    AS des_plan,
    CASE --market_category
        WHEN market_category='Big Screen' AND
             des_manufact IN('SIERRA WIRELESS','WAVECOM','ERICSSON','U-BLOX','TELLINK',
                             'VERIFONE','WESTERMO','QISDA','SIEMENS','SIMCOM','HUAWEI')           THEN tacs.des_manufact
        WHEN market_category='Dongles' AND
             des_manufact IN('SIERRA WIRELESS','HUAWEI','ZTE','NOVATEL',
                             'SIMCOM','QUECTEL','OPTION')                                         THEN tacs.des_manufact
        WHEN market_category IN ('Feature Phones', 'Smartphones') AND
             des_manufact IN ('SAMSUNG','MICROSOFT','APPLE','SONY','LG','BLACKBERRY','BQ-MALATA',
                              'MOTOROLA','HUAWEI','ALCATEL','HTC','ZTE','WIKO','SIEMENS','KAZAM') THEN tacs.des_manufact
        ELSE 'Otros'
    END                                                                        AS des_manufact,
    tacs.des_model,
    IF(os IN('Proprietary','Android','iOS','Serie40','BlackBerry',
             'Windows Phone','Windows Mobile','Firefox'), os, 'Otros')         AS os,
    IF(technology IS NULL, 'SIN CRUCE', UPPER(technology))                     AS technology,
    IF(market_category IN('Big Screen', 'Dongles','Feature Phones',
                          'Smartphones', 'Tablets'), market_category, 'Otros') AS market_category,
    months_old,
    SUM(kpis_agg.n_lines_total)                                                AS n_lines_total,
    SUM(kpis_agg.n_lines_principal)                                            AS n_lines_principal,
    SUM(kpis_agg.n_lines_exceed)                                               AS n_lines_exceed,
    SUM(kpis_agg.n_lines_data_tariff)                                          AS n_lines_data_tariff,
    SUM(kpis_agg.n_lines_extra_data)                                           AS n_lines_extra_data,
    SUM(kpis_agg.n_lines_exceed_extra)                                         AS n_lines_exceed_extra,
    SUM(kpis_agg.n_voice_calls)                                                AS n_voice_calls,
    SUM(kpis_agg.vl_data_bundled)                                              AS vl_data_bundled,
    SUM(kpis_agg.vl_data_exceed)                                               AS vl_data_exceed,
    SUM(kpis_agg.vl_data_consumed)                                             AS vl_data_consumed,
    SUM(kpis_agg.vl_voice_consumed)                                            AS vl_voice_consumed,
    SUM(kpis_agg.vl_sms_consumed)                                              AS vl_sms_consumed,
    SUM(kpis_agg.quota_agg_rv)                                                 AS quota_agg_rv,
    SUM(kpis_agg.quota_data_rv)                                                AS quota_data_rv,
    SUM(kpis_agg.quota_voice_rv)                                               AS quota_voice_rv,
    SUM(kpis_agg.quota_mess_rv)                                                AS quota_mess_rv,
    SUM(kpis_agg.traffic_agg_rv)                                               AS traffic_agg_rv,
    SUM(kpis_agg.traffic_data_rv)                                              AS traffic_data_rv,
    SUM(kpis_agg.traffic_voice_rv)                                             AS traffic_voice_rv,
    SUM(kpis_agg.traffic_mess_rv)                                              AS traffic_mess_rv,
    SUM(kpis_agg.roaming_rv)                                                   AS roaming_rv,
    SUM(kpis_agg.sva_rv)                                                       AS sva_rv,
    SUM(kpis_agg.packs_rv)                                                     AS packs_rv,
    SUM(kpis_agg.top_up_ex_rv)                                                 AS top_up_ex_rv,
    SUM(kpis_agg.top_up_co_rv)                                                 AS top_up_co_rv,
    SUM(kpis_agg.gb_camp_rv)                                                   AS gb_camp_rv,
    SUM(kpis_agg.others_rv)                                                    AS others_rv,
    SUM(kpis_agg.tot_rv)                                                       AS tot_rv,
    SUM(kpis_agg.top_up_rv)                                                    AS top_up_rv,
    SUM(kpis_agg.itx_rv)                                                       AS itx_rv,
    SUM(kpis_agg.exp_itx_rv)                                                   AS exp_itx_rv,
    SUM(kpis_agg.extra_data_rv)                                                AS extra_data_rv,
    SUM(kpis_agg.total_invoice_rv)                                             AS total_invoice_rv,
    kpis_agg.gbic_op_id_pt,
    kpis_agg.month_pt
FROM (
    SELECT
        kpis.prod_type_cd,
        kpis.multisim_ind,
        kpis.gbic_tariff_id,
        kpis.gbic_op_id_pt,
        kpis.month_pt,
        kpis.seg_global_id_pt,
        IF (zones.loc_lev_7  IS NULL, 'SIN CRUCE', zones.loc_lev_7) AS loc_lev_7,
        kpis.device_id,
        kpis.months_old,
        kpis.bta_ind,
        SUM(kpis.n_lines_total)                                     AS n_lines_total,
        IF (kpis.multisim_ind=0, SUM(kpis.n_lines_total), 0)        AS n_lines_principal,
        SUM(kpis.n_lines_exceed)                                    AS n_lines_exceed,
        SUM(kpis.n_lines_data_tariff)                               AS n_lines_data_tariff,
        SUM(kpis.n_lines_extra_data)                                AS n_lines_extra_data,
        SUM(kpis.n_lines_exceed_extra)                              AS n_lines_exceed_extra,
        SUM(kpis.n_voice_calls)                                     AS n_voice_calls,
        SUM(kpis.vl_data_bundled)                                   AS vl_data_bundled,
        SUM(kpis.vl_data_exceed)                                    AS vl_data_exceed,
        SUM(kpis.vl_data_consumed)                                  AS vl_data_consumed,
        SUM(kpis.vl_voice_consumed)                                 AS vl_voice_consumed,
        SUM(kpis.vl_sms_consumed)                                   AS vl_sms_consumed,
        SUM(kpis.quota_agg_rv)                                      AS quota_agg_rv,
        SUM(kpis.quota_data_rv)                                     AS quota_data_rv,
        SUM(kpis.quota_voice_rv)                                    AS quota_voice_rv,
        SUM(kpis.quota_mess_rv)                                     AS quota_mess_rv,
        SUM(kpis.traffic_agg_rv)                                    AS traffic_agg_rv,
        SUM(kpis.traffic_data_rv)                                   AS traffic_data_rv,
        SUM(kpis.traffic_voice_rv)                                  AS traffic_voice_rv,
        SUM(kpis.traffic_mess_rv)                                   AS traffic_mess_rv,
        SUM(kpis.roaming_rv)                                        AS roaming_rv,
        SUM(kpis.sva_rv)                                            AS sva_rv,
        SUM(kpis.packs_rv)                                          AS packs_rv,
        SUM(kpis.top_up_ex_rv)                                      AS top_up_ex_rv,
        SUM(kpis.top_up_co_rv)                                      AS top_up_co_rv,
        SUM(kpis.gb_camp_rv)                                        AS gb_camp_rv,
        SUM(kpis.others_rv)                                         AS others_rv,
        SUM(kpis.tot_rv)                                            AS tot_rv,
        SUM(kpis.top_up_rv)                                         AS top_up_rv,
        SUM(kpis.itx_rv)                                            AS itx_rv,
        SUM(kpis.exp_itx_rv)                                        AS exp_itx_rv,
        SUM(kpis.extra_data_rv)                                     AS extra_data_rv,
        SUM(kpis.total_invoice_rv)                                  AS total_invoice_rv
    FROM
        kpis_mobile kpis
        LEFT JOIN dims_geo_zones zones
          ON kpis.gbic_op_id_pt    = zones.gbic_op_id_pt AND
             kpis.month_pt         = zones.month_pt      AND
             kpis.gbic_geo_zone_id = zones.gbic_geo_zone_id
    WHERE 
        kpis.seg_global_id_pt = 6 AND
        kpis.gbic_op_id_pt    = 1
    GROUP BY
        kpis.prod_type_cd,
        kpis.multisim_ind,
        kpis.gbic_tariff_id,
        kpis.gbic_op_id_pt,
        kpis.month_pt,
        kpis.seg_global_id_pt,
        concat (kpis.gbic_op_id, kpis.seg_global_id ),
        IF (zones.loc_lev_7 IS NULL, 'SIN CRUCE', zones.loc_lev_7),
        kpis.device_id,
        kpis.months_old,
        kpis.bta_ind
    ) kpis_agg
    LEFT JOIN (
        SELECT
            gbic_tariff_id,
            is_data_tariff,
            des_plan,
            gbic_op_id_pt,
            month_pt
        FROM
            dims_m_tariffs_history
        WHERE 
            hist_flag<>'S'
    ) tarif 
    ON kpis_agg.gbic_op_id_pt  = tarif.gbic_op_id_pt AND
       kpis_agg.month_pt       = tarif.month_pt      AND
       kpis_agg.gbic_tariff_id = tarif.gbic_tariff_id
    LEFT JOIN (
        SELECT
            t.device_id,
            t.des_manufact,
            t.des_model,
            t.os,
            t.technology,
            h.market_category,
            h.tef_category,
            t.month_pt
        FROM
            dims_tacs t
        INNER JOIN dims_tacs_history h
        ON t.month_pt  = h.month_pt AND
           t.device_id = h.device_id
    ) tacs
    ON kpis_agg.month_pt  = tacs.month_pt AND
       kpis_agg.device_id = tacs.device_id
    GROUP BY
        kpis_agg.month_pt,
        kpis_agg.gbic_op_id_pt,
        kpis_agg.bta_ind,
        kpis_agg.loc_lev_7,
        kpis_agg.prod_type_cd,
        IF(kpis_agg.gbic_tariff_id IS NULL, -1, kpis_agg.gbic_tariff_id),
        CASE
          WHEN tarif.is_data_tariff IS NULL THEN 'SIN CRUCE'
          WHEN tarif.is_data_tariff=0       THEN 'SIN DATOS'
          WHEN tarif.is_data_tariff=1       THEN 'CON DATOS'
        END,
        IF(tarif.des_plan IS NULL, 'SIN CRUCE', tarif.des_plan),
        CASE
          WHEN market_category='Big Screen' AND
               des_manufact IN('SIERRA WIRELESS','WAVECOM','ERICSSON','U-BLOX','TELLINK',
                               'VERIFONE','WESTERMO','QISDA','SIEMENS','SIMCOM','HUAWEI')           THEN tacs.des_manufact
          WHEN market_category='Dongles' AND
               des_manufact IN('SIERRA WIRELESS','HUAWEI','ZTE','NOVATEL',
                               'SIMCOM','QUECTEL','OPTION')                                         THEN tacs.des_manufact
          WHEN market_category IN ('Feature Phones', 'Smartphones') AND
               des_manufact IN ('SAMSUNG','MICROSOFT','APPLE','SONY','LG','BLACKBERRY','BQ-MALATA',
                                'MOTOROLA','HUAWEI','ALCATEL','HTC','ZTE','WIKO','SIEMENS','KAZAM') THEN tacs.des_manufact
          ELSE 'Otros'
        END,
        des_model,
        IF(os IN('Proprietary','Android','iOS','Serie40','BlackBerry','Windows Phone','Windows Mobile','Firefox'), os, 'Otros'),
        IF(technology IS NULL, 'SIN CRUCE', UPPER(technology)),
        IF(market_category IN('Big Screen', 'Dongles', 'Feature Phones', 'Smartphones', 'Tablets'), market_category, 'Otros'),
        months_old;
