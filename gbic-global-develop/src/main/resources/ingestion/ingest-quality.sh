#!/usr/bin/env bash
# 
# gplatform ingestion quality process
# -----------------------------------
# 
# Takes files from ${LOCAL_INBOX_PER_OB} and distributes them by name into HDFS
# It has to be executed as admin user.
# It has to have write access to local service_checks directory, p.e: { service_checks }/{OB}/{DATASET}
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/ingestion/ingest-quality.sh {version} {country} {dataset-list} [{yyyyMM} [{local_path_prefix}]]
# 
# Example:
# 
# # ${GPLATFORM_HOME}/ingestion/ingest-files.sh MSv5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201501
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] VERSION COUNTRY DATASET_LIST [ MONTH [LOCAL_PATH_PREFIX] ]"
  echo -e "Takes files from local inboxes per OB and distributes them by name into HDFS\n"
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized COUNTRY format:"
  echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
  echo -e "\nRecognized DATASET_LIST format:"
  echo -e "\tSpace separated list of uppercase dataset names"
  echo -e "\nRecognized MONTH format:"
  echo -e "\tYYYYMM"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-common.sh
source ${GBIC_HOME}/common/gbic-gplatform-env.sh

ALARM_URL=${INGEST_ALARM_URL}

# Finalizes script with an error code and an alarm. It receives two arguments:
# - ERROR_CODE: numeric code of the error for ending the script
# - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
# Example:
#   die 27 "There is not connection to server"
die () {
  __die "${ALARM_URL}"    \
        ":hamburger:"     \
        "$1"              \
        "$2"              \
        "${SERVICE_NAME}" \
        "Data Quality Ingestion script for '${OB_2M}', '${DATASETS}' and ${CONTENT_DATE}"
}


# OPTIONS: 
###################################################################################################

while getopts '\-:' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        help           ) help;;
        # "--" terminates argument processing
        ''             ) break;;
        *              ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# ARGS: VERSION, OB_2M, DATASET_LIST, CONTENT_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
###################################################################################################
# Validate VERSION
# -------------------------------------------------------------------------------------------------
VERSION=$1
if [[ "${VERSION}" == "" ]]; then
  cancel "wrong Version of file to be generated: NOT RECEIVED"
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
# Validate DATASET
# -------------------------------------------------------------------------------------------------
DATASETS=$3
if [[ "$DATASETS" == "" ]]; then
  cancel "wrong Dataset list: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate MONTH
# -------------------------------------------------------------------------------------------------
MONTH=$4
if [[ "${MONTH}" == "" ]]; then
  LOAD_DATE=`date '+%Y%m'`
  if [[ "${OB_2M}" == "ES" ]]; then
    CONTENT_DATE=$(date "--date=${LOAD_DATE}01 -2 month" +"%Y%m")
  else
    CONTENT_DATE=$(date "--date=${LOAD_DATE}01 -1 month" +"%Y%m")
  fi
else
  CONTENT_DATE=${MONTH}
fi
# -------------------------------------------------------------------------------------------------
# Validate LOCAL_PATH_PREFIX
# -------------------------------------------------------------------------------------------------
PREFIX=$5
if [[ "${PREFIX}" == "" ]]; then
    LOCAL_PATHS_PREFIX=${DEFAULT_LOCAL_PATH_PREFIX}
else
    LOCAL_PATHS_PREFIX=${PREFIX}
fi


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`

CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}

# {LOCAL_PATH_PREFIX}/{OB3M}/ + GPLATFORM/{VERS}/{YYYY}{MM}/{OB2M}_{DATASET}_{YYYY}{MM}.bz2
#                                                                |_ {OB2M}_{DATASET}_{YYYY}{MM}.txt
LOCAL_INBOX_PER_OB="GPLATFORM/${VERSION}"

TEMP_LOCAL_PATH_PER_OB="${LOCAL_INBOX_PER_OB}/TEMP"
LOGFILE_NAME=log-ingest-files-${EXEC_TIME//[ ]/_}.log
LOGFILE_NAME=${LOGFILE_NAME//[:]/-}
LOGFILE_LOCAL_PATH_PER_OB=${TEMP_LOCAL_PATH_PER_OB}
LOGFILE_HDFS_PATH=${HDFS_SRVCHECKS}/_ingestion-logs

SUFFIX=QUALITY

# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched ingestion-quality script for ${VERSION} on ${EXEC_TIME}"
debug "Logs will be available on ${LOCAL_PATHS_PREFIX}/${OB_2M}/${LOGFILE_LOCAL_PATH_PER_OB}/${LOGFILE_NAME}"
debug "OPTIONS:"
debug "ARGUMENTS:"
debug "|- Semantic Model Version of QUALITY files to be ingested: '${VERSION}' (${VERSION})"
debug "|- Country Code of QUALITY files to be ingested: '${OB_2M}'"
debug "|- QUALITY files to be ingested: '${DATASETS}'."
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date of QUALITY file to be ingested not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date of QUALITY file to be ingested: '${CONTENT_DATE}'"
fi
if [[ "${PREFIX}" == "" ]]; then
  # Console message
  debug "|- Root folder for QUALITY file to be ingested not specified. Default will be used: '${LOCAL_PATHS_PREFIX}'"
else
  # Console message
  debug "|- Root folder for QUALITY file to be ingested: '${LOCAL_PATHS_PREFIX}'"
fi

# -------------------------------------------------------------------------------------------------
# Processing of OB.
# -------------------------------------------------------------------------------------------------
OB_3m=$(getOB_3m ${OB_2M})
OB_3M=$(getOB_3M ${OB_2M})
OP_ID=$(getOpId  ${OB_2M})

BZ2_FILE_PATH=${LOCAL_PATHS_PREFIX}/${OB_3M}/${LOCAL_INBOX_PER_OB}/${CONTENT_DATE}
TEMP_LOCAL_PATH=${LOCAL_PATHS_PREFIX}/${OB_3M}/${TEMP_LOCAL_PATH_PER_OB}
LOGFILE_LOCAL_PATH=${LOCAL_PATHS_PREFIX}/${OB_3M}/${LOGFILE_LOCAL_PATH_PER_OB}

LOG_FILE=${LOGFILE_LOCAL_PATH}/${LOGFILE_NAME}

# Console message
debug "${OB_3M} logs: tail -F \`ls -rt ${LOGFILE_LOCAL_PATH}/log-ingest-files-*_*.log | tail -1\`"

mkdir -p ${TEMP_LOCAL_PATH}
mkdir -p ${LOGFILE_LOCAL_PATH}

# Log message
info "Searching QUALITY files of ${OB_3M} for ${CONTENT_DATE} in ${BZ2_FILE_PATH}. Launched at $EXEC_TIME" > ${LOG_FILE} 2>&1

for DATASET in ${DATASETS}; do
    
    # Log message
    info "Processing ${DATASET}'s QUALITY" >> ${LOG_FILE} 2>&1
    
    DATASET_LOWERCASE=$(echo "${DATASET}" | awk '{print tolower($0)}')
    HDFS_PATH="${HDFS_SRVCHECKS}/${OB_3m}/${DATASET_LOWERCASE}/day=${CONTENT_YEAR}-${CONTENT_MONTH}-01"
    
    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${HDFS_PATH}
    if hadoop fs -test -e ${HDFS_PATH}/_ERROR ; then
        hadoop fs -rm -skipTrash ${HDFS_PATH}/_ERROR > /dev/null 2>&1
    fi
    
    # Check if BZ2 file(s) have arrived
    BZ2_FILE_GLOB=${BZ2_FILE_PATH}/${OB_2M}_${DATASET}*_${CONTENT_DATE}_${SUFFIX}.bz2
    NUM_FILES=`ls ${BZ2_FILE_GLOB} 2> /dev/null | wc -l`
    if [ "${NUM_FILES}" -eq "0" ]; then
        ERR_MSG="File(s) not found"
        # Log message
        error "${ERR_MSG} for ${OB_3M}'s ${DATASET} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01" >> ${LOG_FILE} 2>&1
        # Control file on HDFS
        echo $ERR_MSG > ${TEMP_LOCAL_PATH}/.tmp_error
        hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${TEMP_LOCAL_PATH}/.tmp_error ${HDFS_PATH}/_ERROR
        rm -f ${TEMP_LOCAL_PATH}/.tmp_error
    else
        # Check if the file was previously copied into HDFS
        if hadoop fs -test -z ${HDFS_PATH}/_SUCCESS ; then
            # Log message
            warn "File ${HDFS_PATH} already exists. It will be skipped" >> ${LOG_FILE} 2>&1
        else
            BZ2_FILE_LIST=`ls ${BZ2_FILE_GLOB} 2> /dev/null`
            ERRORS=0
            for BZ2_FILE in ${BZ2_FILE_LIST}; do
                
                BZ2_FILE_WITHOUT_PATH=$(basename $BZ2_FILE)
                NUM_FILES=`ls ${BZ2_FILE} 2> /dev/null | wc -l`
                if [ "${NUM_FILES}" -eq "0" ]; then
                    ERR_MSG="File not found ${BZ2_FILE_WITHOUT_PATH}"
                    # Log messages
                    warn "Check ${TEMP_LOCAL_PATH} and see why there's no file called ${BZ2_FILE_WITHOUT_PATH}" >> ${LOG_FILE} 2>&1
                    error "${ERR_MSG} for ${OB_3M}'s ${DATASET} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01" >> ${LOG_FILE} 2>&1
                    # Control file on HDFS
                    echo $ERR_MSG > ${TEMP_LOCAL_PATH}/.tmp_error
                    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${TEMP_LOCAL_PATH}/.tmp_error ${HDFS_PATH}/_ERROR
                    rm -f ${TEMP_LOCAL_PATH}/.tmp_error
                    ERRORS=$((ERRORS + 1))
                    break
                else
                    # Log message
                    info "Putting ${BZ2_FILE_WITHOUT_PATH} file to HDFS" >> ${LOG_FILE} 2>&1
                    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${BZ2_FILE} ${HDFS_PATH}/${BZ2_FILE_WITHOUT_PATH}
                    # Log message
                    info "File ${BZ2_FILE} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01 SUCCESSFULLY UPLOADED" >> ${LOG_FILE} 2>&1
                fi
            done
            if [[ "${ERRORS}" == "0" ]]; then
                hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -touchz ${HDFS_PATH}/_SUCCESS
            fi
        fi
    fi
    # Log message
    info "${DATASET} processed" >> ${LOG_FILE} 2>&1
done

log_file_hdfs=${LOGFILE_HDFS_PATH}/${OB_3M}-${LOGFILE_NAME}
# Log message
info "Finished processing QUALITY files of ${OB_3M}. Log file will be available on HDFS: ${log_file_hdfs}" >> ${LOG_FILE} 2>&1

# BACKUP LOG TO HDFS
hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${LOGFILE_HDFS_PATH}
hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${LOG_FILE} ${log_file_hdfs}

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
