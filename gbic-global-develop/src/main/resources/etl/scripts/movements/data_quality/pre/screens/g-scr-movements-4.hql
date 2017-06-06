-------------------------------------------------------------------------------
--- NUMBER OF MOVEMENTS CUSTOMER_ID NOT IN CUSTOMER TABLE
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of movements with customer_id field
---              not in customer table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'movements' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 4 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=movements;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=4;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_customer'                            AS field,
    join_customer                              AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT movements.*,
    IF ( customer.customer_id IS NOT NULL, 'ok', 'ko' ) AS join_customer
    FROM (
        SELECT
            customer_id,
            month,
            gbic_op_id
        FROM gbic_dq_movements
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS movements
    LEFT OUTER JOIN (
        SELECT
            distinct customer_id,
            month,
            country_id
        FROM gbic_dq_customer_for_movements
        WHERE country_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS customer
    ON movements.customer_id = customer.customer_id
      AND movements.month = customer.month
      AND movements.gbic_op_id = customer.country_id
)z 
GROUP BY join_customer;
