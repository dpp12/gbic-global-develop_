USE {{ db.schema_dq }};

{% for item in project.interfaces %}
-- ****************************************************************************
-- LOAD DATA FOR {{ item }}
-- ****************************************************************************
LOAD DATA LOCAL INFILE '{{ remote.service }}/etl/scripts/{{ item }}/data_quality/mysql/dq_file_entity_{{ item }}.csv'
  INTO TABLE file_entity
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '{{ remote.service }}/etl/scripts/{{ item }}/data_quality/mysql/dq_screen_{{ item }}.csv'
  INTO TABLE screen
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '{{ remote.service }}/etl/scripts/{{ item }}/data_quality/mysql/dq_test_{{ item }}.csv'
  INTO TABLE test
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  IGNORE 1 LINES;


{% endfor %}
