-------------------------------------------------------------------------------
--- NUMBER OF M_LINES CUSTOMER_ID NOT IN CUSTOMER
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of m_lines with customer_id field
---              not in customer table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 7 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=7;

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
    SELECT
        m_lines.*,
        IF ( customer.customer_id IS NOT NULL, 'ok', 'ko' ) AS join_customer
    FROM (
        SELECT
            customer_id,
            month,
            gbic_op_id
        FROM gbic_dq_m_lines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
          AND (
                  (
                      pre_post_id != 'P'
                      AND (
                          gbic_op_id = 1   OR
                          gbic_op_id = 201 OR
                          gbic_op_id = 3
                      )
                  )
                  OR (
                      gbic_op_id = 2 OR
                      gbic_op_id = 5 OR
                      gbic_op_id = 9 OR
                      gbic_op_id = 8
                  )
              )
    ) AS m_lines
    LEFT OUTER JOIN (
        SELECT
            distinct customer_id,
            month,
            gbic_op_id
        FROM gbic_dq_customer_for_mlines
    ) AS customer
      ON m_lines.customer_id = customer.customer_id
) z
GROUP BY join_customer;
