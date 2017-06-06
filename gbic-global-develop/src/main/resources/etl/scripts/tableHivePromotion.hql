-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ item }};

-------------------------------------------------------------------------------
-- GOLD ZONE AREA
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global;

ALTER TABLE gbic_global_{{ item }}
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);
ALTER TABLE gbic_global_{{ item }}
ADD PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);
!hdfs dfs -rm -f -r /apps/hive/warehouse/{{ project.prefix }}gbic_global.db/gbic_global_${fileName}/gbic_op_id=${gbic_op_id}/month=${nominalTime};
!hdfs dfs -mv /apps/hive/warehouse/{{ project.prefix }}gbic_global_staging.db/gbic_global_${fileName}/gbic_op_id=${gbic_op_id}/month=${nominalTime} /apps/hive/warehouse/{{ project.prefix }}gbic_global.db/gbic_global_${fileName}/gbic_op_id=${gbic_op_id}/month=${nominalTime};


-------------------------------------------------------------------------------
-- STAGING AREA
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_staging;

ALTER TABLE gbic_global_{{ item }}
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);
