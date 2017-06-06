#!/usr/bin/env bash
# 
# pre-requirements historic ingestion
# ------------------------------------
# 
# Launch pre-requirements ingestion processes for the OB given as argument.
# Executes the ingestion of the previously generated files for the OB from a start-date to an end-date.
# 
# Usage:
# 
# # /opt/gbic/services/gplatform/global/prereqs/prereqs-ingestion.sh {vers-num} {country} {file-list} {start-date} {end-date} {local_paths_prefix}
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/prereqs/prereqs-ingestion.sh 5 ES "DIM_M_BILLING_CYCLE DIM_M_GROUP_SVA" 201501 201605 {{ remote.genfileoutbox }}
# 
###################################################################################################

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/../.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-common.sh

# ARGS: MS_VERSION, COUNTRY, LIST_OF_FILES, START_DATE Format yyyyMM, END_DATE Format yyyyMM, LOCAL_PATHS_PREFIX
###################################################################################################
# Validate MS_VERSION
# -------------------------------------------------------------------------------------------------
MS_VERSION=$1
if [[ "${MS_VERSION}" == "" ]]; then
  cancel "wrong Semantic Model Version of file to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate OB
# -------------------------------------------------------------------------------------------------
OB_2M=$2
validateOB ${OB_2M}; validOB=$?
if [[ "${OB_2M}" == "" ]]; then
  cancel "wrong Country Code of file to be generated: NOT RECEIVED"
elif [[ "$validOB" = "0" ]]; then
  cancel "wrong Country Code of file to be generated: '${OB_2M}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_OBS_2M//[ ]/, } }."
fi
# -------------------------------------------------------------------------------------------------
# Validate LIST_OF_FILES
# -------------------------------------------------------------------------------------------------
LIST_OF_FILES="$3"
if [[ "${LIST_OF_FILES}" == "" ]]; then
  cancel "wrong list of files to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate START_DATE
# -------------------------------------------------------------------------------------------------
START_DATE=$4
if [[ "${START_DATE}" == "" ]]; then
  cancel "wrong start date of files to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate END_DATE
# -------------------------------------------------------------------------------------------------
END_DATE=$5
if [[ "${END_DATE}" == "" ]]; then
  cancel "wrong end date of files to be generated: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate LOCAL_PATHS_PREFIX
# -------------------------------------------------------------------------------------------------
LOCAL_PATHS_PREFIX=$6
if [[ "${LOCAL_PATHS_PREFIX}" == "" ]]; then
  cancel "wrong local_paths_prefix: NOT RECEIVED"
fi

# CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
END_LOOP_DATE=${END_DATE}01
iterator_date=${START_DATE}01
OB_3M=$(getOB_3M ${OB_2M})
LOCAL_INBOX=${OB_3M}/GPLATFORM/MSv${MS_VERSION}

# PROCESS FILES
###################################################################################################
info "Start historic pre-requirements ingestion for ${OB_2M} from ${START_DATE} to ${END_DATE}"
while [[ $iterator_date -le $END_LOOP_DATE ]]; do
  month=$(date --date "$iterator_date" +%Y%m)
  
  info "Launching pre-requirements ingestion for ${OB_2M} files: ${LIST_OF_FILES} on ${month}"
  sh ${GBIC_HOME}/ingestion/ingest-files.sh ${MS_VERSION} ${OB_2M} "${LIST_OF_FILES}" ${month} ${LOCAL_PATHS_PREFIX}
  
  exitCode=$?
  if [[ "${exitCode}" != "0" ]]; then
    die 1 "File ingestion failed with exit code ${exitCode}"
  else
    # workaround to the lack of return code of ingest-files script
    ingestionLog=`ls -rt ${LOCAL_PATHS_PREFIX}/${LOCAL_INBOX}/TEMP/log-ingest-files-*_*.log | tail -1`
    exitCode=`grep '#ERROR' ${ingestionLog} | wc -l`
    if [[ "${exitCode}" != "0" ]]; then
      die 2 "`grep '#ERROR' ${ingestionLog} | head -1 | gawk '{ $1=""; $2=""; $3=""; print $0 }' | sed 's/^[ ]*//g'`"
    fi
  fi
  
  iterator_date=$(date "--date=${iterator_date} +1 month" +"%Y%m%d")
done

info "Finished historic pre-requirements ingestion process for ${OB_2M} ------------------ "
