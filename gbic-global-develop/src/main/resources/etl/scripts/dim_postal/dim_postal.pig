/* dim_postal_cd.pig
 * -----------------
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

/* Organizative segment homogeneization:
 * yyyy-MM-dd,2,LOCATION,1,AUT,COMUNIDAD AUTONOMA,4,Location level 4
 * yyyy-MM-dd,2,LOCATION,1,CPO,CÓDIGO POSTAL,1,Location level 1
 * ...
 */
location = LOAD '{{ cluster.service }}/homog/month=$nominalTime/dim=2/*'
    USING PigStorage(',')
    AS (loc_month:        chararray,
        loc_concept_id:   int,
        loc_concept_name: chararray,
        loc_gbic_op_id:   int,
        loc_local_cd:     chararray,
        loc_local_name:   chararray,
        loc_global_id:    int,
        loc_global_name:  chararray
    );

in_data = LOAD '{{ hdfs.inbox }}/$ob/$version/DIM_POSTAL/month=$nominalTime/*'
    USING PigStorage('|')
    AS (country_id:     int,
        month_id:       chararray,
        postal_id:      chararray,
        location_level: chararray,
        location_name:  chararray
    );

unique_data   = DISTINCT in_data;
noheader_data = FILTER unique_data BY month_id!='MONTH_ID';

gbic_global_data = JOIN
    noheader_data BY (country_id) LEFT OUTER,
    gbic_op_ids   BY (gbic_op_id);

gbic_filter_obs = FILTER gbic_global_data BY gbic_op_id IS NOT NULL;

gbic_global_data_segment = JOIN
    gbic_filter_obs BY (country_id, location_level) LEFT OUTER,
    location        BY (loc_gbic_op_id, loc_local_cd);

store_data = FOREACH gbic_global_data_segment GENERATE
    gbic_op_name                                          AS (gbic_op_name:               chararray),
    postal_id                                             AS (postal_id:                  chararray),
    location_level                                        AS (location_level_local_cd:    chararray),
    (loc_local_name IS NULL? 'UNKNOWN': loc_local_name)   AS (location_level_local_name:  chararray),
    (loc_global_id IS NULL? -1: loc_global_id)            AS (location_level_global_id:   int),
    (loc_global_name IS NULL? 'UNKNOWN': loc_global_name) AS (location_level_global_name: chararray),
    location_name                                         AS (location_name:              chararray),
    gbic_op_id                                            AS (gbic_op_id:                 int),
    '$nominalTime'                                        AS (month:                      chararray);

STORE store_data INTO '{{ project.prefix }}gbic_global_staging.gbic_global_dim_postal'
    USING org.apache.hive.hcatalog.pig.HCatStorer;
