-------------------------------------------------------------------------------
-- HIVE VARIABLES
-------------------------------------------------------------------------------
-- SET hivevar:ob=esp;
-- SET hivevar:version=MSv5;
-- SET hivevar:upperFileName=M_LINE_SERVICES;
-- SET hivevar:nominalTime=2015-10-01;
-- SET hivevar:gbic_op_id=1;
-- SET hivevar:fileName={{ interface }};

-------------------------------------------------------------------------------
-- DATA QUALITY AREA
-------------------------------------------------------------------------------
-- DDL for {{ interface }} quality tables
--     * External table: gbic_dq_{{ interface }}_ext
--     * Consolidation table: gbic_dq_{{ interface }}
-------------------------------------------------------------------------------
USE {{ project.prefix }}gbic_global_dq;

-------------------------------------------------------------------------------
-- Drop external partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_m_line_services_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

-------------------------------------------------------------------------------
-- Drop orc partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_m_line_services
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

-------------------------------------------------------------------------------
-- Drop service_checks partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_service_checks
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
);

-------------------------------------------------------------------------------
-- Drop file_test partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_file_test
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
);

-------------------------------------------------------------------------------
-- Drop screen_results partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_screen_results
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
);

-------------------------------------------------------------------------------
-- Drop test_results partition
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_test_result
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    file = '${fileName}',
    day = '${nominalTime}'
);

-------------------------------------------------------------------------------
-- Drop auxiliary tables partitions
-------------------------------------------------------------------------------
ALTER TABLE gbic_dq_dim_m_services_for_mlineservices_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_services_for_mlineservices
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_group_sva_for_mlineservices_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_dim_m_group_sva_for_mlineservices
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_m_lines_for_mlineservices_ext
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);

ALTER TABLE gbic_dq_m_lines_for_mlineservices
DROP IF EXISTS PARTITION (
    gbic_op_id = ${gbic_op_id},
    month = '${nominalTime}'
);
