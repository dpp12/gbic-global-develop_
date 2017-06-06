-------------------------------------------------------------------------------
--- NUMBER OF IMEI_SALES POSTAL_CD NOT IN DIM_POSTAL
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of imei_sales with postal_cd field not
---              in dim_postal table
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'imei_sales' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 6 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=imei_sales;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=6;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'join_dim_postal'                          AS field,
    join_dim_postal                            AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT
        imei_sales.*,
        IF ( postal.postal_id IS NOT NULL OR imei_sales.postal_cd = '-1', 'ok', 'ko' ) AS join_dim_postal
    FROM (
        SELECT
            postal_cd,
            month,
            gbic_op_id
        FROM gbic_dq_imei_sales
        WHERE gbic_op_id = ${gbic_op_id}
          AND month = '${nominalTime}'
    ) AS imei_sales
    LEFT OUTER JOIN (
        SELECT
            distinct postal_id,
            month,
            gbic_op_id
        FROM gbic_dq_dim_postal_for_mlines
    ) AS postal
      ON imei_sales.postal_cd = postal.postal_id
) z
GROUP BY join_dim_postal;
