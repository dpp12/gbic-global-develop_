/* multisim.pig
 * -------------
 */
/* Global operators:
 * 1,MOVISTAR ESPAÑA,es,esp,EUR
 * 2,MOVISTAR ARGENTINA,ar,arg,ARS
 * ...
 * 5,MOVISTAR PERU,pe,per,PEN
 * ...
 * 201,VIVO BRASIL,br,bra,BRL
 * ...
 */
gbic_op_ids = LOAD '{{ cluster.common }}/GBICGlobalOperators.csv'
    USING PigStorage(',')
    AS (gbic_op_id:       int,
        gbic_op_name:     chararray,
        gbic_op_cd1:      chararray,
        gbic_op_cd2:      chararray,
        gbic_op_currency: chararray
    );

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/MULTISIM/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:  int,
        month_id:    chararray,
        msisdn_main: chararray,
        msisdn_add:  chararray
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

store_data = FOREACH gbic_filter_obs GENERATE
    gbic_op_name   AS (gbic_op_name: chararray),
    msisdn_main    AS (msisdn_main:  chararray),
    msisdn_add     AS (msisdn_add:   chararray),
    gbic_op_id     AS (gbic_op_id:   int),
    '$nominalTime' AS (month:        chararray);

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_multisim'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
