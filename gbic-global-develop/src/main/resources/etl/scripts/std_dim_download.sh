#!/usr/bin/env bash
# 
# DIM files download
# ------------------
# 
# Download data from MySQL's tables
# - {{ db.schema_homog }}.gbic_global_std_concepts
# - {{ db.schema_homog }}.gbic_global_std_global_concepts
# - {{ db.schema_homog }}.gbic_global_std_local_concepts
# and put it to hdfs' path {{ cluster.service }}/homog
# 
# Usage:
# 
# # sudo -u hdfs {{ remote.service }}/etl/std_dim_download.sh [yyyyMM]
# 
###################################################################################################


# CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
TMP_FOLDER=/tmp
HDFS_UMASK={{ hdfs.umask }}
HDFS_TARGET={{ cluster.service }}/homog
MYSQL_HOST={{ db.host }}
MYSQL_USER={{ db.user }}
MYSQL_PORT={{ db.port }}
MYSQL_SCHEMA={{ db.schema_homog }}
TBL_CONCEPTS=GBIC_GLOBAL_STD_CONCEPTS
TBL_GLOBAL_VALUES=GBIC_GLOBAL_STD_GLOBAL_CONCEPTS
TBL_LOCAL_MAPPINGS=GBIC_GLOBAL_STD_LOCAL_CONCEPTS


# ARGS: CONTENT_DATE (optional) - Format yyyyMM
###################################################################################################
execTime=`date '+%F %H:%M:%S'`

if [[ "$1" == "" ]]; then
  LOAD_DATE=`date '+%Y%m'`
  CONTENT_DATE=$(date "--date=${LOAD_DATE}01 -1 month" +"%Y%m")
  echo "`date '+%F %H:%M:%S'` #INFO  Argument not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  CONTENT_DATE=$1
  echo "`date '+%F %H:%M:%S'` #INFO  Argument '${CONTENT_DATE}' received. "
fi


# INITIALIZATION
###################################################################################################
echo "`date '+%F %H:%M:%S'` #INFO  Init GPlatform STD DIM Download $execTime"

DATE="${CONTENT_DATE:0:4}-${CONTENT_DATE:4:2}-01"

QUERY1="SELECT DISTINCT concept_id FROM ${MYSQL_SCHEMA}.${TBL_LOCAL_MAPPINGS} WHERE date_ini <= '${DATE}' AND date_end  >= '${DATE}' ORDER BY concept_id;"
CONCEPTS=`echo $QUERY1 | mysql -h ${MYSQL_HOST} -P${MYSQL_PORT} -u ${MYSQL_USER} -sN -p{{ db.pass }}`

for CONCEPT in ${CONCEPTS}; do
  
  echo "`date '+%F %H:%M:%S'` #INFO  Downloading month=${DATE}/dim=${CONCEPT} file"
  
  QUERY2="SELECT '${DATE}' AS month_active, t1.concept_id, t1.concept_name, t3.gbic_op_id, t3.local_cd, t3.local_desc, t2.global_id, t2.global_desc FROM ${MYSQL_SCHEMA}.${TBL_LOCAL_MAPPINGS} t3 LEFT OUTER JOIN ${MYSQL_SCHEMA}.${TBL_GLOBAL_VALUES} t2 ON t3.concept_id = t2.concept_id AND t3.global_id = t2.global_id LEFT OUTER JOIN ${MYSQL_SCHEMA}.${TBL_CONCEPTS} t1 ON t3.concept_id = t1.concept_id WHERE t1.concept_id=${CONCEPT} AND t3.date_ini <= '${DATE}' AND t3.date_end  >= '${DATE}' ORDER BY t3.gbic_op_id, t3.local_cd;"
  echo ${QUERY2} | mysql -h ${MYSQL_HOST} -P${MYSQL_PORT} -u ${MYSQL_USER} -sN -p{{ db.pass }} | sed 's/\t/,/g' > ${TMP_FOLDER}/tmp_${CONCEPT}_${DATE}
  
  echo "`date '+%F %H:%M:%S'` #INFO  Putting it to hdfs"
  
  hadoop fs -rm -r -skipTrash "${HDFS_TARGET}/month=${DATE}/dim=${CONCEPT}" > /dev/null 2>&1
  hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${HDFS_TARGET}/month=${DATE}/dim=${CONCEPT}
  hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${TMP_FOLDER}/tmp_${CONCEPT}_${DATE} ${HDFS_TARGET}/month=${DATE}/dim=${CONCEPT}/data-00000
  
  rm ${TMP_FOLDER}/tmp_${CONCEPT}_${DATE}
  
  echo "`date '+%F %H:%M:%S'` #INFO  Cleaning temp files"
  
  # In order to be accessible via Hive, this must be executed:
  # USE ${HIVE_DATABASE};
  # CREATE EXTERNAL TABLE IF NOT EXISTS gbic_global_dim_homog (month_active String, concept_id int, concept_name String, gbic_op_id int, local_cd String, local_desc String, global_id int, global_desc String) PARTITIONED BY (month String, dim int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
  # ALTER TABLE gbic_global_dim_homog DROP IF EXISTS PARTITION (month='${DATE}', dim=${CONCEPT});
  # ALTER TABLE gbic_global_dim_homog ADD PARTITION (month='${DATE}', dim=${CONCEPT}) LOCATION '${HDFS_TARGET}/month=${DATE}/dim=${CONCEPT}';
  
done

echo "`date '+%F %H:%M:%S'` #INFO  Ended GPlatform STD DIM Download"
