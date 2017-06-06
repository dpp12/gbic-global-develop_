-------------------------------------------------------------------------------
--- NUMBER OF CONSUMER PARTY_IDENTIFICATION_NUM NOT ANONYMIZED
-------------------------------------------------------------------------------
--- 
--- Description: Gets the number of records of consumer segment with
---              party_identification_num field whose length is not the
---              expected according to encryption applied by the country
--- Parameters:
--- 
---     gbic_op_id:    Country internal identification
---     fileName:      Name of the processed file. 'customer' in this case
---     nominalTime:   Date of the file in format YYYY-mm-dd
---     screenType:    Part of the screen Id. 'G' in this case
---     screenCounter: Part of the screen Id. 5 in this case
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-----------------------------
---   VARIABLES SETUP     ---
-----------------------------
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName=customer;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:screenType=G;
-- SET hivevar:screenCounter=4;

-----------------------------
---   QUERY EXECUTION     ---
-----------------------------
INSERT OVERWRITE TABLE gbic_dq_screen_results
PARTITION ( gbic_op_id, file, day, screenId )
SELECT
    'party_id_num_length'                      AS field,
    length(party_identification_num)           AS fieldContent,
    count(*)                                   AS fieldValue,
    ${gbic_op_id}                              AS gbic_op_id,
    '${fileName}'                              AS file,
    '${nominalTime}'                           AS day,
    concat('${screenType}','${screenCounter}') AS screenId
FROM (
    SELECT party_identification_type_cd,
           party_identification_num,
           upper(segment_cd) AS segment,
           gbic_op_id,
           month
    FROM gbic_dq_customer
    WHERE gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) file
JOIN (
    SELECT seg_local_cd,
           seg_gbic_op_id,
           seg_global_id,
           month
    FROM gbic_dq_segments_for_customer
    WHERE seg_gbic_op_id = ${gbic_op_id}
      AND month = '${nominalTime}'
) seg
  ON file.segment = seg.seg_local_cd

WHERE file.gbic_op_id = seg.seg_gbic_op_id
  AND file.month = seg.month
  AND seg.seg_global_id = 6
  AND NOT (file.gbic_op_id = 2 AND file.party_identification_type_cd IN ('CU','08'))
  AND NOT (file.gbic_op_id = 1 AND file.party_identification_type_cd = 'C')

GROUP BY length(party_identification_num);
