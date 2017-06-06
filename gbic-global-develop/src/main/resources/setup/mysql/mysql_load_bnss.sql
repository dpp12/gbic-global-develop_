USE {{ db.schema }};

LOAD DATA LOCAL INFILE '{{ remote.service }}/setup/mysql/data/gbic_global_operators.csv'
  INTO TABLE gbic_global_operators
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"';
