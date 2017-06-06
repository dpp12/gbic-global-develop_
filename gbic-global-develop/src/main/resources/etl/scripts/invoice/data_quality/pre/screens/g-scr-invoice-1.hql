-------------------------------------------------------------------------------
--- NUMBER OF INVOICE NULL VALUES
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of invoice with
---              one of its values as a null value.
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'invoice' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 1 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=invoice;
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
        IF (  country_id       IS NULL OR
              month_id         IS NULL OR
              customer_id      IS NULL OR
              msisdn_id        IS NULL OR
              activation_dt    IS NULL OR
              billing_cycle_id IS NULL OR
              quota_data_rv    IS NULL OR
              quota_voice_rv   IS NULL OR
              quota_mess_rv    IS NULL OR
              quota_agg_rv     IS NULL OR
              traffic_data_rv  IS NULL OR
              traffic_voice_rv IS NULL OR
              traffic_mess_rv  IS NULL OR
              traffic_agg_rv   IS NULL OR
              roaming_rv       IS NULL OR
              sva_rv           IS NULL OR
              packs_rv         IS NULL OR
              top_up_ex_rv     IS NULL OR
              top_up_co_rv     IS NULL OR
              gb_camp_rv       IS NULL OR
              others_rv        IS NULL OR
              tot_rv           IS NULL OR
              top_up_rv        IS NULL OR
              itx_rv           IS NULL OR
              exp_itx_rv       IS NULL OR
              total_invoice_rv IS NULL OR
              subscription_id  IS NULL,
          'ko',
          'ok') AS null_fields
    FROM gbic_dq_invoice
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
GROUP BY null_fields;
