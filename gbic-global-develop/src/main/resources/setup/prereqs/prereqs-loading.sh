#!/usr/bin/env bash
# 
# pre-requirements historic loading
# ---------------------------------
# 
# Launch pre-requirements processes for all the OBs.
# Executes pre-requirement processes for the different OBs from a start-date to an end-date without the ingestion to hdfs option (due to the time taken). 
# Then, launch the ingestion process of the generated files in background.
# 
# Usage:
# 
# # /opt/gbic/services/gplatform/global/prereqs/prereqs-loading.sh {vers-num} {country-list} {start-date} {end-date} {local_path_prefix}
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/prereqs/prereqs-loading.sh 5 "ES AR BR CL PE" 201501 201605 {{ remote.genfileoutbox }}
# 
###################################################################################################

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/../.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-common.sh

# ARGS: MS_VERSION, COUNTRY_LIST (optional), START_DATE (optional) Format yyyyMM, END_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
###################################################################################################
# Validate MS_VERSION
# -------------------------------------------------------------------------------------------------
MS_VERSION=$1
if [[ "${MS_VERSION}" == "" ]]; then
  cancel "wrong Semantic Model Version of file to be generated: NOT RECEIVED"
fi
# Validate LIST_OF_OB2M
# -------------------------------------------------------------------------------------------------
LIST_OF_OB2M=$2
if [[ "${LIST_OF_OB2M}" == "" ]]; then
  cancel "wrong list of countries to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate START_DATE
# -------------------------------------------------------------------------------------------------
START_DATE=$3
if [[ "${START_DATE}" == "" ]]; then
  cancel "wrong start date of files to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate END_DATE
# -------------------------------------------------------------------------------------------------
END_DATE=$4
if [[ "${END_DATE}" == "" ]]; then
  cancel "wrong end date of files to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate LOCAL_PATH_PREFIX
# -------------------------------------------------------------------------------------------------
LOCAL_PATHS_PREFIX=$5
if [[ "${LOCAL_PATHS_PREFIX}" == "" ]]; then
  cancel "wrong local_paths_prefix: NOT RECEIVED"
fi

# CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
execTime=`date '+%F %H:%M:%S'`
iterator_date=${START_DATE}01
END_LOOP_DATE=${END_DATE}01
LOGFILE_NAME=log-prereqs-loading-${execTime//[ ]/_}
LOGFILE_NAME=${LOGFILE_NAME//[:]/-}
LOGFILE_PATH=${LOCAL_PATHS_PREFIX}/${ob3M}/${LOGFILE_NAME}

# PROCESS FILES
###################################################################################################
info "Start historic pre-requirements loading for ${LIST_OF_OBS} from ${START_DATE} to ${END_DATE}"
while [[ $iterator_date -le $END_LOOP_DATE ]]; do
  for ob in ${LIST_OF_OB2M}; do
    month=$(date --date "$iterator_date" +%Y%m)
    
    info "Launching pre-requirements for ${ob} files: \"$(getGenFiles ${ob})\" on ${month}"
    ${GBIC_HOME}/etl/prereqs/prereqs.sh ${MS_VERSION} ${ob} "$(getGenFiles ${ob})" $month ${LOCAL_PATHS_PREFIX}
    rc=$?;
    if [[ $rc != 0 ]]; then
      error "Fail to launch pre-requirements for ${ob} files: \"$(getGenFiles ${ob})\" on ${iterator_date}"
      exit $rc;
    fi
  done
  iterator_date=$(date "--date=${iterator_date} +1 month" +"%Y%m%d")
done

#Launch processes in background (one per OB) in order to ingest the previously generated files
for ob in ${LIST_OF_OB2M}; do
  info "Launching background process for ingestion of ${ob}"
  LOG_FILE=${LOGFILE_PATH}-${ob}.log
  nohup sh ${GBIC_HOME}/setup/prereqs/prereqs-ingestion.sh ${MS_VERSION} ${ob} "$(getGenFiles ${ob})" ${START_DATE} ${END_DATE} ${LOCAL_PATHS_PREFIX} > ${LOG_FILE} &
done

info "Finished historic pre-requirements loading process ------------------ "
warn "Some proccesses of ingestion might be still running in background"
