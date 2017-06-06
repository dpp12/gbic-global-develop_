/* global_tacs.pig
 * ---------------
 */
in_data = LOAD '{{ cluster.common }}/tacs/month=$nominalTime/*'
    USING PigStorage('|')
    AS (tac:                chararray,
        volume:             long,
        technology_4g_sp:   chararray,
        technology_4g_br:   chararray,
        technology_4g_mx:   chararray,
        technology_4g_ch:   chararray,
        technology_4g_ur:   chararray,
        technology_4g_pe:   chararray,
        technology_4g_ar:   chararray,
        des_manufact:       chararray,
        des_brand:          chararray,
        des_model:          chararray,
        market_category:    chararray,
        tef_category:       chararray,
        touchscreen:        chararray,
        keyboard:           chararray,
        os:                 chararray,
        version_os:         chararray,
        technology_2g:      chararray,
        technology_3g:      chararray,
        technology_4g_dl:   chararray,
        technology_4g_ul:   chararray,
        gsm_850:            chararray,
        gsm_900:            chararray,
        gsm_1800:           chararray,
        gsm_1900:           chararray,
        umts_850:           chararray,
        umts_900:           chararray,
        umts_1700:          chararray,
        umts_1900:          chararray,
        umts_2100:          chararray,
        lte_band_17:        chararray,
        lte_band_12:        chararray,
        lte_band_20:        chararray,
        lte_band_5:         chararray,
        lte_band_8:         chararray,
        lte_band_4:         chararray,
        lte_band_3:         chararray,
        lte_band_2:         chararray,
        lte_band_1:         chararray,
        lte_band_7:         chararray,
        lte_band_33:        chararray,
        lte_band_34:        chararray,
        lte_band_38:        chararray,
        lte_band_28:        chararray,
        lte_band_13:        chararray,
        lte_band_25:        chararray,
        lte_band_40:        chararray,
        lte_band_42:        chararray,
        lte_band_31:        chararray,
        size_display:       chararray,
        resolution_display: chararray,
        colors_display:     chararray,
        camera:             chararray,
        bluetooth:          chararray,
        wifi_a:             chararray,
        wifi_b:             chararray,
        wifi_g:             chararray,
        wifi_n:             chararray,
        battery_type:       chararray,
        battery_capacity:   chararray,
        processor:          chararray,
        sim:                chararray,
        sim_form:           chararray,
        isdb_tb_1_seg_req:  chararray,
        mob_int_channel:    chararray,
        embms:              chararray,
        analog_or_digital:  chararray,
        ipv6_support:       chararray
);

filtered_data = FILTER in_data BY tac!='TAC';

gbic_tacs_arrays = FOREACH filtered_data GENERATE
    *,
    TOKENIZE(des_model, '",()*/') AS des_model_list;

store_data = FOREACH gbic_tacs_arrays {
  GENERATE
    (int) tac          AS (tac:                int),
    volume             AS (volume:             long),
    des_manufact       AS (des_manufact:       chararray),
    des_brand          AS (des_brand:          chararray),
    des_model_list     AS (des_model:          bag{t:tuple(t1:chararray)}),
    market_category    AS (market_category:    chararray),
    tef_category       AS (tef_category:       chararray),
    touchscreen        AS (touchscreen:        chararray),
    keyboard           AS (keyboard:           chararray),
    os                 AS (os:                 chararray),
    version_os         AS (version_os:         chararray),
    technology_2g      AS (technology_2g:      chararray),
    technology_3g      AS (technology_3g:      chararray),
    technology_4g_dl   AS (technology_4g_dl:   chararray),
    technology_4g_ul   AS (technology_4g_ul:   chararray),
    gsm_850            AS (gsm_850:            chararray),
    gsm_900            AS (gsm_900:            chararray),
    gsm_1800           AS (gsm_1800:           chararray),
    gsm_1900           AS (gsm_1900:           chararray),
    umts_850           AS (umts_850:           chararray),
    umts_900           AS (umts_900:           chararray),
    umts_1700          AS (umts_1700:          chararray),
    umts_1900          AS (umts_1900:          chararray),
    umts_2100          AS (umts_2100:          chararray),
    lte_band_17        AS (lte_band_17:        chararray),
    lte_band_12        AS (lte_band_12:        chararray),
    lte_band_20        AS (lte_band_20:        chararray),
    lte_band_5         AS (lte_band_5:         chararray),
    lte_band_8         AS (lte_band_8:         chararray),
    lte_band_4         AS (lte_band_4:         chararray),
    lte_band_3         AS (lte_band_3:         chararray),
    lte_band_2         AS (lte_band_2:         chararray),
    lte_band_1         AS (lte_band_1:         chararray),
    lte_band_7         AS (lte_band_7:         chararray),
    lte_band_33        AS (lte_band_33:        chararray),
    lte_band_34        AS (lte_band_34:        chararray),
    lte_band_38        AS (lte_band_38:        chararray),
    lte_band_28        AS (lte_band_28:        chararray),
    lte_band_13        AS (lte_band_13:        chararray),
    lte_band_25        AS (lte_band_25:        chararray),
    lte_band_40        AS (lte_band_40:        chararray),
    lte_band_42        AS (lte_band_42:        chararray),
    lte_band_31        AS (lte_band_31:        chararray),
    size_display       AS (size_display:       chararray),
    resolution_display AS (resolution_display: chararray),
    colors_display     AS (colors_display:     chararray),
    camera             AS (camera:             chararray),
    bluetooth          AS (bluetooth:          chararray),
    wifi_a             AS (wifi_a:             chararray),
    wifi_b             AS (wifi_b:             chararray),
    wifi_g             AS (wifi_g:             chararray),
    wifi_n             AS (wifi_n:             chararray),
    battery_type       AS (battery_type:       chararray),
    battery_capacity   AS (battery_capacity:   chararray),
    processor          AS (processor:          chararray),
    sim                AS (sim:                chararray),
    sim_form           AS (sim_form:           chararray),
    isdb_tb_1_seg_req  AS ( isdb_tb_1_seg_req: chararray),
    mob_int_channel    AS (mob_int_channel:    chararray),
    embms              AS (embms:              chararray),
    analog_or_digital  AS (analog_or_digital:  chararray),
    ipv6_support       AS (ipv6_support:       chararray),
    technology_4g_sp   AS (technology_4g_sp:   chararray),
    technology_4g_br   AS (technology_4g_br:   chararray),
    technology_4g_mx   AS (technology_4g_mx:   chararray),
    technology_4g_ch   AS (technology_4g_ch:   chararray),
    technology_4g_ur   AS (technology_4g_ur:   chararray),
    technology_4g_pe   AS (technology_4g_pe:   chararray),
    technology_4g_ar   AS (technology_4g_ar:   chararray),
    '$nominalTime'     AS (month:              chararray);
}

STORE store_data INTO '{{ project.prefix }}GBIC_GLOBAL.GBIC_GLOBAL_TACS'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
