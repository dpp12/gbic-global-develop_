USE {{ db.schema_dq }};

LOAD DATA LOCAL INFILE '{{ remote.service }}/setup/mysql/data_quality/data/dq_country.csv'
  INTO TABLE country
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '{{ remote.service }}/setup/mysql/data_quality/data/dq_project.csv'
  INTO TABLE project
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  IGNORE 1 LINES;
