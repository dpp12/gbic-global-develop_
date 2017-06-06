-------------------------------------------------------------------------------
--- RECORDS WITHOUT CUSTOMERS
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of not matching any customers
---
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'invoice' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 3 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=invoice;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=3;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_customers'                           AS field,
    join_customers                             AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        invoices.*,
        IF (custs.customer_id IS NOT NULL, 'ok', 'ko') AS join_customers
    FROM (
        SELECT customer_id,
               month,
               gbic_op_id
        FROM gbic_dq_invoice
        WHERE gbic_op_id = ${gbic_op_id}
         AND month = '${nominalTime}'
         AND month_id != 'MONTH_ID'
         AND NOT (tot_rv = 0)
         AND NOT ((top_up_rv > 0) OR (TOP_UP_CO_RV > 0))
    ) AS invoices
    LEFT OUTER JOIN 
    (
        SELECT customer_id,
               month,
               country_id
        FROM gbic_dq_customer_for_invoice
        WHERE country_id = ${gbic_op_id}
          AND month_id != 'MONTH_ID'
          AND month = '${nominalTime}'
    ) AS custs
      ON  invoices.gbic_op_id = custs.country_id
      AND invoices.customer_id = custs.customer_id
) AS customers
GROUP BY join_customers;
