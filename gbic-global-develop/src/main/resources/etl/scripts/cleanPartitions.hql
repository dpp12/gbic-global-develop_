USE  {{ project.prefix }}GBIC_GLOBAL;

ALTER TABLE GBIC_GLOBAL_CUSTOMER DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DAILY_TRAFFIC DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DIM_F_TARIFF_PLAN DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DIM_F_VOICE_TYPE DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DIM_M_GROUP_SVA DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DIM_M_TARIFF_PLAN DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DIM_M_SERVICES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_DIM_POSTAL DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_F_ACCESS DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_F_LINES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_F_TARIFF_PLAN DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_HANDSET_UPGRADE DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_IMEI_SALES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_INVOICE DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_M_LINE_SERVICES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_M_LINES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_MOVEMENTS DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_MULTISIM DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
-- @deprecated services_line is deprecated. Use m_line_services instead.
ALTER TABLE GBIC_GLOBAL_SERVICES_LINE DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_TACS DROP IF EXISTS PARTITION (month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_TRAFFIC_DATA DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_TRAFFIC_SMS DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_TRAFFIC_VOICE DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
