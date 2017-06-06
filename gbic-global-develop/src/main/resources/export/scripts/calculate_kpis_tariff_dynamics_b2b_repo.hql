USE {{ project.prefix }}gbic_global_bnss;

SET hive.execution.engine=mr;
SET hive.auto.convert.join=false;

INSERT INTO TABLE kpis_tariff_dynamics_agg_b2b PARTITION ( status='ACTIVE', month='${hiveconf:curdate}', gbic_op_id=${hiveconf:ob} )

SELECT
    curr.seg_global_id                                  AS seg_global_id,
    curr.seg_global_name                                AS seg_global_name,
    IF (prev.tariff_plan_id  IS NULL,
        "NEW_COMMER",
        prev.tariff_plan_id)                            AS tariff_plan_id_prev,
    IF (prev.tariff_plan_des IS NULL,
        "NEW_COMMER",
        prev.tariff_plan_des)                           AS tariff_plan_des_prev,
    curr.tariff_plan_id                                 AS tariff_plan_id_curr,
    curr.tariff_plan_des                                AS tariff_plan_des_curr,
    mov.mov_grp_local_cd                                AS movement_local_cd, 
    mov.mov_grp_local_name                              AS movement_local_name,  
    mov.mov_grp_global_id                               AS movement_global_id, 
    mov.mov_grp_global_name                             AS movement_global_name,
    count(*)                                            AS num_lines,
    IF (sum(prev.total_rv) IS NULL,
        0.0,
        sum(prev.total_rv))                             AS total_rv_prev,
    sum(curr.total_rv)                                  AS total_rv_curr,
    IF (percentile_approx(prev.total_rv, 0.05) IS NULL,
        0.0,
        percentile_approx(prev.total_rv, 0.05))         AS percen05_prev,
    IF (percentile_approx(prev.total_rv, 0.25) IS NULL,
        0.0,
        percentile_approx(prev.total_rv, 0.25))         AS percen25_prev,
    IF (percentile_approx(prev.total_rv, 0.50) IS NULL,
        0.0,
        percentile_approx(prev.total_rv, 0.50))         AS percen50_prev,
    IF (percentile_approx(prev.total_rv, 0.75) IS NULL,
        0.0,
        percentile_approx(prev.total_rv, 0.75))         AS percen75_prev,
    IF (percentile_approx(prev.total_rv, 0.95) IS NULL,
        0.0,
        percentile_approx(prev.total_rv, 0.95))         AS percen95_prev,
    percentile_approx(curr.total_rv, 0.05)              AS percen05_curr,
    percentile_approx(curr.total_rv, 0.25)              AS percen25_curr,
    percentile_approx(curr.total_rv, 0.50)              AS percen50_curr,
    percentile_approx(curr.total_rv, 0.75)              AS percen75_curr,
    percentile_approx(curr.total_rv, 0.95)              AS percen95_curr,
    IF (prev.tariff_plan_id IS NULL,
        "NEW_COMMER",
        IF (prev.tariff_plan_id == curr.tariff_plan_id,
            "PERMANENT",
            "REPO"))                                    AS type

FROM (
    SELECT
        gbic_op_id,
        month,
        seg_global_id,
        seg_global_name,
        tariff_plan_id,
        tariff_plan_des,
        total_rv,
        msisdn_id,
        IF( ${hiveconf:ob} != 3 , subscription_id, 'DUMMY ') as subscription_id
    FROM {{ project.prefix }}gbic_global.gbic_global_m_lines curr1
    WHERE gbic_op_id=${hiveconf:ob}
      AND month='${hiveconf:curdate}'
      AND seg_global_id IN (1,2,3,4,5,9)
) AS curr

LEFT OUTER JOIN (
    SELECT
        gbic_op_id,
        month,
        seg_global_id,
        seg_global_name,
        tariff_plan_id,
        tariff_plan_des,
        total_rv,
        msisdn_id
    FROM {{ project.prefix }}gbic_global.gbic_global_m_lines prev1
    WHERE gbic_op_id=${hiveconf:ob}
      AND month='${hiveconf:prevdate}'
      AND seg_global_id IN (1,2,3,4,5,9)
) AS prev
  ON  curr.gbic_op_id=prev.gbic_op_id
  AND curr.msisdn_id=prev.msisdn_id
LEFT OUTER JOIN 
 (
  SELECT IF( ${hiveconf:ob} != 3 , mov1.subscription_id, 'DUMMY ') as subscription_id,
         mov1.msisdn_id, 
         mov1.mov_grp_local_cd, 
         mov1.mov_grp_local_name, 
         mov1.mov_grp_global_id, 
         mov1.mov_grp_global_name
  FROM {{ project.prefix }}gbic_global.gbic_global_movements mov1
  WHERE gbic_op_id=${hiveconf:ob}
    AND month='${hiveconf:curdate}'
    AND mov_grp_global_id IN (-1,7)
) AS mov 
ON curr.msisdn_id = mov.msisdn_id
AND curr.subscription_id = mov.subscription_id

GROUP BY
    curr.gbic_op_id,
    curr.month,
    curr.seg_global_id,
    curr.seg_global_name,
    prev.tariff_plan_id,
    prev.tariff_plan_des,
    curr.tariff_plan_id,
    curr.tariff_plan_des,
    mov.mov_grp_local_cd, 
    mov.mov_grp_local_name, 
    mov.mov_grp_global_id, 
    mov.mov_grp_global_name;
