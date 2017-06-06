USE {{ project.prefix }}gbic_global_bnss;

SET hive.execution.engine=mr;
SET hive.auto.convert.join=false;

INSERT INTO TABLE kpis_tariff_dynamics_agg_b2b PARTITION ( status='INACTIVE', month='${hiveconf:curdate}', gbic_op_id=${hiveconf:ob} )

SELECT
    chur.seg_global_id                                  AS seg_global_id,
    chur.seg_global_name                                AS seg_global_name,
    chur.tariff_plan_id                                 AS tariff_plan_id_prev,
    chur.tariff_plan_des                                AS tariff_plan_des_prev,
    "CHURNER"                                           AS tariff_plan_id_curr,
    "CHURNER"                                           AS tariff_plan_des_curr,
    mov.mov_grp_local_cd                                AS movement_local_cd, 
    mov.mov_grp_local_name                              AS movement_local_name,  
    mov.mov_grp_global_id                               AS movement_global_id, 
    mov.mov_grp_global_name                             AS movement_global_name,
    count(*)                                            AS num_lines,
    sum(chur.total_rv)                                  AS total_rv_prev,
    0.0                                                 AS total_rv_curr,
    percentile_approx(chur.total_rv, 0.05)              AS percen05_prev,
    percentile_approx(chur.total_rv, 0.25)              AS percen25_prev,
    percentile_approx(chur.total_rv, 0.50)              AS percen50_prev,
    percentile_approx(chur.total_rv, 0.75)              AS percen75_prev,
    percentile_approx(chur.total_rv, 0.95)              AS percen95_prev,
    0.0                                                 AS percen05_curr,
    0.0                                                 AS percen25_curr,
    0.0                                                 AS percen50_curr,
    0.0                                                 AS percen75_curr,
    0.0                                                 AS percen95_curr,
    "CHURNER"                                           AS type

FROM (
 SELECT prev.gbic_op_id,
        prev.month,
        prev.seg_global_id,
        prev.seg_global_name,
        prev.tariff_plan_id,
        prev.tariff_plan_des,
        prev.total_rv,
        prev.msisdn_id,
        IF( ${hiveconf:ob} != 3 , prev.subscription_id, 'DUMMY ') as subscription_id
    FROM {{ project.prefix }}gbic_global.gbic_global_m_lines prev
    WHERE prev.gbic_op_id=${hiveconf:ob}
      AND prev.month='${hiveconf:prevdate}'
      AND prev.seg_global_id IN (1,2,3,4,5,9)
      AND prev.msisdn_id NOT IN 
      (
       SELECT DISTINCT msisdn_id
       FROM {{ project.prefix }}gbic_global.gbic_global_m_lines curr
       WHERE curr.gbic_op_id=${hiveconf:ob}
          AND curr.month='${hiveconf:curdate}'
       )
) AS chur
 LEFT OUTER JOIN 
 (
  SELECT IF( ${hiveconf:ob} != 3 , subscription_id, 'DUMMY ') as subscription_id, 
         msisdn_id, 
         mov_grp_local_cd, 
         mov_grp_local_name, 
         mov_grp_global_id, 
         mov_grp_global_name
  FROM {{ project.prefix }}gbic_global.gbic_global_movements 
  WHERE gbic_op_id=${hiveconf:ob}
    AND month='${hiveconf:curdate}'
    AND mov_grp_global_id IN (-1,3,4,8)
) AS mov 
ON chur.msisdn_id = mov.msisdn_id
AND chur.subscription_id = mov.subscription_id
GROUP BY
    chur.gbic_op_id,
    chur.month,
    chur.seg_global_id,
    chur.seg_global_name,
    chur.tariff_plan_id,
    chur.tariff_plan_des,
    mov.mov_grp_local_cd, 
    mov.mov_grp_local_name, 
    mov.mov_grp_global_id, 
    mov.mov_grp_global_name;
