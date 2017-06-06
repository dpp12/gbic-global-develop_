USE {{ db.schema_dq }};

-- Get id of file_entity for given dataset
SELECT id_fileentity
INTO @id_fileentity
FROM file_entity
WHERE file_name = @filename;

-- Get id of file_instance for given dataset, country and date
SELECT id_fileinstance
INTO @id_fileinstance
FROM file_instance
WHERE id_fileentity = @id_fileentity
  AND gbic_op_id = @gbic_op_id
  AND content_dt = @date;

-- Get id of file_revision, taking last revision executed
SELECT id_filerevision
INTO @id_filerevision
FROM file_revision
WHERE id_fileinstance = @id_fileinstance
  AND file_process_date = (
    SELECT MAX(file_process_date)
    FROM file_revision
    WHERE id_fileinstance = @id_fileinstance
  )
  AND file_revision_num = (
    SELECT MAX(file_revision_num)
    FROM file_revision
    WHERE id_fileinstance = @id_fileinstance
  )
;

-- Get count of error tests
SELECT
  tt.test_desc AS test,
  tr.test_expected_value AS expected_value,
  tr.test_resulting_value AS resulting_value,
  CONCAT(tr.error_threshold, '%') AS threshold,
  tr.test_state AS result
FROM (
  SELECT *
  FROM test_result
  WHERE id_filerevision = @id_filerevision
) tr
LEFT JOIN (
  SELECT *
  FROM test
) tt
ON tr.id_test = tt.id_test;
