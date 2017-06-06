-------------------------------------------------------------------------------
--- RECORDS WITHOUT CONDITION
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records (%) of not matching with condition:
---              total_month_rv-(voice_month_rv+tv_month_rv+bband_month_rv
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'f_lines' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 7 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=f_lines;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=8;

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
    SELECT
        f_lines.*,
        IF (f_lines.difference < 0.1, 'ok', 'ko') AS count_revenues
    FROM (
        SELECT ABS(total_month_rv-(voice_month_rv+tv_month_rv+bband_month_rv)) AS difference
        FROM gbic_dq_f_lines
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
          AND gbic_op_id != 1
    ) f_lines
) AS revenues
GROUP BY count_revenues;
