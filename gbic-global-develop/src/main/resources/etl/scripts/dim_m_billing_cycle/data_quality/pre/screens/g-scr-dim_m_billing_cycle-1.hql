-------------------------------------------------------------------------------
--- NUMBER OF DIM_M_BILLING_CYCLE NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of dim_m_billing_cycle with
---              one of its values as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'dim_m_billing_cycle' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=201;
-- SET hivevar:fileName=dim_m_billing_cycle;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=1;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'null_fields'                              AS field,
    null_fields                                AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        *,
        IF ( country_id             IS NULL OR
             billing_cycle_month    IS NULL OR
             billing_cycle_id       IS NULL OR
             billing_cycle_des      IS NULL OR
             billing_cycle_start_dt IS NULL OR
             billing_cycle_end_dt   IS NULL OR
             billing_due_dt         IS NULL OR
             billing_rv_computes    IS NULL,
           'ko',
           'ok') AS null_fields
    FROM gbic_dq_dim_m_billing_cycle
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
