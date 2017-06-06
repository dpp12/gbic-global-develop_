-------------------------------------------------------------------------------
--- NUMBER OF RECORDS WHERE total_rv != (no_top_up_rv+top_up_cost_rv)
-------------------------------------------------------------------------------

--- Description: Gets the number of records not matching the condition
---              total_rv = (no_top_up_rv+top_up_cost_rv)
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'm_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=m_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=5;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'count_revenues'                           AS field,
    count_revenues                             AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT m_lines.*,
    IF ( m_lines.difference < 0.1, 'ok', 'ko' ) AS count_revenues
    FROM (
        SELECT ABS(total_rv-(no_top_up_rv+top_up_cost_rv)) AS difference
        FROM gbic_dq_m_lines
        WHERE gbic_op_id = ${gbic_op_id} 
          AND month = '${nominalTime}'
    ) m_lines
) AS revenues
GROUP BY count_revenues;
