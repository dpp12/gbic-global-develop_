/* dim_customer.pig
 * ------------------
 * 
 */

in_data = LOAD '/user/gplatform/inbox/bra/DIM_CUSTOMER/month=$nominalTime/DIM_CUSTOMER-$nominalTime.csv'
    USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','YES_MULTILINE')
    AS (id_cli:          long,
        customer_id_mov: long,
        customer_id_fjo: long,
        assinatura:      long,
        nrc_prnc:        long,
        codigo_postal:   int,
        ds_grau_esco:    chararray,
        socio_econ:      chararray,
        ds_estd_cvil:    chararray,
        fecha_actlz:     chararray
        );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data   BY fecha_actlz!='FECHA_ACTLZ';

store_data = FOREACH noheader_data  GENERATE
    'VIVO BRASIL'    AS (gbic_op_name:    chararray),
    id_cli           AS (id_cli:          long),
    customer_id_mov  AS (customer_id_mov: long),
    customer_id_fjo  AS (customer_id_fjo: long),
    assinatura       AS (assinatura:      long),
    nrc_prnc         AS (nrc_prnc:        long),
    codigo_postal    AS (codigo_postal:   int),
    ds_grau_esco     AS (ds_grau_esco:    chararray),
    socio_econ       AS (socio_econ:      chararray),
    ds_estd_cvil     AS (ds_estd_cvil:    chararray),
    fecha_actlz      AS (fecha_actlz:     chararray),
    201              AS (gbic_op_id:      int),
    '$nominalTime'   AS (month:           chararray);

STORE store_data INTO 'GBIC_GLOBAL.GBIC_GLOBAL_BRA_DIM_CUSTOMER'
    USING org.apache.hcatalog.pig.HCatStorer();
