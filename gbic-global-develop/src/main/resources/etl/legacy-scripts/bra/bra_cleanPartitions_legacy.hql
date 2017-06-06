USE GBIC_GLOBAL;

ALTER TABLE GBIC_GLOBAL_BRA_CUSTOMER DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_DIM_CUSTOMER DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_DIM_SERVICES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_FATURA DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_INF_LINE_DEVICE DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_INTERCON DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_LINE_SERVICES DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_MOV_DEV DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_RECARGA DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_TRAFEGO DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
ALTER TABLE GBIC_GLOBAL_BRA_TRAFEGO_DADOS DROP IF EXISTS PARTITION (gbic_op_id='${op}', month='${nominalTime}');
