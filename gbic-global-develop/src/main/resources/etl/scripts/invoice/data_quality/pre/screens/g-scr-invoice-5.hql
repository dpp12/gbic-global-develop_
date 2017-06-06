-------------------------------------------------------------------------------
--- NUMBER OF INVOICE BILLING_CYCLE_ID NOT IN DIM_M_BILLING_CYCLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of invoice with
---              billing_cycle_id field not in dim_m_billing_cycle table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'invoice' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=invoice;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=5;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_billing'                             AS field,
    join_billing                               AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        invoice.*,
        IF (bill.billing_cycle_id IS NOT NULL, 'ok', 'ko') AS join_billing
    FROM (
        SELECT
            billing_cycle_id,
            month,
            gbic_op_id
        FROM gbic_dq_invoice
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
          -- do not include prepaid lines
          AND (billing_cycle_id != '-1'   AND gbic_op_id IN (1,201,3) --   -1 for esp, bra, chl
            OR billing_cycle_id != 'A_99' AND gbic_op_id IN (2)       -- A_99 for arg
            OR billing_cycle_id != '0'    AND gbic_op_id IN (5)       --    0 for per
          ) 
    ) AS invoice
    LEFT OUTER JOIN (
        SELECT
            country_id,
            billing_cycle_month,
            billing_cycle_id
        FROM gbic_dq_dim_m_billing_cycle_for_invoice
    ) AS bill
      ON  invoice.gbic_op_id = bill.country_id
      AND invoice.billing_cycle_id = bill.billing_cycle_id
) z
GROUP BY join_billing;
