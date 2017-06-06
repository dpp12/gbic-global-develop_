#!/usr/bin/env bash
# 
# ingest-TRAFFIC-files
# --------------------
# 
# Ingest ${INTERFACE} grouping PK for ES into correct inbox.
# 
# Takes files from ${LOCAL_PATH_PREFIX}, upload to HDFS and generate merge ${INTERFACE} file
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # /opt/gbic/services/gplatform/global/ingestion/ingest-TRAFFIC-files.sh {vers-num} {country} {interface} [{yyyyMM} [{local_path_prefix}]]
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/ingestion/ingest-TRAFFIC-files.sh 5 ES TRAFFIC_VOICE 201501 {{ remote.inbox }}
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] MS_VERSION COUNTRY DATASET [MONTH [LOCAL_PATH_PREFIX]]"
  echo -e "Ingests specified TRAFFIC file for the specified country and month using some HQL logic.\n"
  echo -e "\t-i, --ingest"
  echo -e "\t               execute ingestion script for specified file in raw location."
  echo -e "\t               It won't be executed by default."
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized COUNTRY format:"
  echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
  echo -e "\nRecognized MONTH format:"
  echo -e "\tYYYYMM"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-common.sh
source ${GBIC_HOME}/common/gbic-gplatform-env.sh

GBIC_INGESTION_PATH=${GBIC_HOME}/ingestion

INGESTION_SCRIPT=${GBIC_INGESTION_PATH}/ingest-files.sh
DEFAULT_LOCAL_PATHS_PREFIX={{ remote.inbox }}

GENERATION_SCRIPT_PATH=${GBIC_INGESTION_PATH}/hive
GENERATION_SCRIPT_PREFFIX=generate-
GENERATION_SCRIPT_SUFFIX=-file.hql

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
        "TRAFFIC file ingestion for ${OB_2M}_${INTERFACE}_${CONTENT_DATE}"
}


# OPTIONS: -i
###################################################################################################
OPT_INGEST=0

while getopts '\-:i' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        ingest         ) OPT_INGEST=1;;
        ingest*        ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        help           ) help;;
        # "--" terminates argument processing
        ''             ) break;;
        *              ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
    i ) OPT_INGEST=1;;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# ARGS: MS_VERSION, OB_2M, DATASET, CONTENT_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
##############################################################################################################
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
# Validate INTERFACE
# -------------------------------------------------------------------------------------------------
INTERFACE=$3
if [[ "${INTERFACE}" == "" ]]; then
  cancel "wrong Interface Name of file to be checked: NOT RECEIVED"
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
    LOCAL_PATHS_PREFIX=${DEFAULT_LOCAL_PATHS_PREFIX}
else
    LOCAL_PATHS_PREFIX=${PREFIX}
fi


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`

OB_3m=$(getOB_3m ${OB_2M})
OB_3M=$(getOB_3M ${OB_2M})
OP_ID=$(getOpId  ${OB_2M})

CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}

GENERATION_SCRIPT_NAME=${GENERATION_SCRIPT_PREFFIX}${INTERFACE}${GENERATION_SCRIPT_SUFFIX}
GENERATION_SCRIPT=${GENERATION_SCRIPT_PATH}/${GENERATION_SCRIPT_NAME}

HDFS_PATH=${HDFS_INBOX}/${OB_3m}/MSv${MS_VERSION}

HDFS_GENERATED_FILE_DIRECTORY=${HDFS_PATH}/${INTERFACE}
HDFS_RAW_FILE_DIRECTORY=${HDFS_PATH}/raw_${INTERFACE}
HDFS_TEMP_DIRECTORY=${HDFS_PATH}/tmp_${INTERFACE}.bak

HDFS_MONTH_FOLDER="month=${CONTENT_YEAR}-${CONTENT_MONTH}-01"


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity
# -------------------------------------------------------------------------------------------------
info "ARGUMENTS:"
info "|- Semantic Model Version of file to be generated: '${MS_VERSION}' (MSv${MS_VERSION})"
info "|- Country Code of file to be generated: '${OB_2M}'"
if [[ "${MONTH}" == "" ]]; then
  info "|- Date of file to be generated not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  info "|- Date of file to be generated: '${CONTENT_DATE}'"
fi
if [[ "${PREFIX}" == "" ]]; then
  info "|- Root folder for file to be generated not specified. Default will be used: '${LOCAL_PATHS_PREFIX}'"
else
  info "|- Root folder for file to be generated: '${LOCAL_PATHS_PREFIX}'"
fi

# -------------------------------------------------------------------------------------------------
# Ingest file
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_INGEST}" == "1" ]]; then
  info "Running file ingestion..."
  
  # Process will be:
  # | 1. Backup, if exists, any generated data for specified month
  # | 2. Ingest raw file
  # | 3. Move ingested file to raw directory
  # | 4. Restore backup if created
  
  # 1. Backup existing generated file
  makebak=0
  if hadoop fs -test -d ${HDFS_GENERATED_FILE_DIRECTORY}/${HDFS_MONTH_FOLDER}; then
    makebak=1
  fi
  if [[ "${makebak}" == "1" ]]; then
    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${HDFS_TEMP_DIRECTORY}
    if hadoop fs -test -d ${HDFS_TEMP_DIRECTORY}/${HDFS_MONTH_FOLDER}; then
      hadoop fs -rm -r ${HDFS_TEMP_DIRECTORY}/${HDFS_MONTH_FOLDER}
    fi
    hadoop fs -mv ${HDFS_GENERATED_FILE_DIRECTORY}/${HDFS_MONTH_FOLDER} ${HDFS_TEMP_DIRECTORY}
  fi
  
  # 2. Ingest raw files
  ${INGESTION_SCRIPT} ${MS_VERSION} ${OB_2M} ${INTERFACE} ${CONTENT_DATE} ${LOCAL_PATHS_PREFIX}
  
  exitCode=$?
  if [[ "${exitCode}" != "0" ]]; then
    die 2 "File ingestion failed with exit code ${exitCode}"
  else
    # workaround to the lack of return code of ingest-files script
    ingestionLog=`ls -rt ${LOCAL_PATHS_PREFIX}/${OB_3M}/GPLATFORM/MSv${MS_VERSION}/TEMP/log-ingest-files-*_*.log | tail -1`
    exitCode=`grep '#ERROR' ${ingestionLog} | wc -l`
    if [[ "${exitCode}" != "0" ]]; then
      die 2 "`grep '#ERROR' ${ingestionLog} | head -1 | gawk '{ $1=""; $2=""; $3=""; print $0 }' | sed 's/^[ ]*//g'`"
    fi
  fi
  
  # 3. Move ingested file to raw directory
  hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${HDFS_RAW_FILE_DIRECTORY}
  if hadoop fs -test -d ${HDFS_RAW_FILE_DIRECTORY}/${HDFS_MONTH_FOLDER}; then
    hadoop fs -rm -r ${HDFS_RAW_FILE_DIRECTORY}/${HDFS_MONTH_FOLDER}
  fi
  hadoop fs -mv ${HDFS_GENERATED_FILE_DIRECTORY}/${HDFS_MONTH_FOLDER} ${HDFS_RAW_FILE_DIRECTORY}
  
  # 4. Restore backup if created
  if [[ "${makebak}" == "1" ]]; then
    hadoop fs -mv ${HDFS_TEMP_DIRECTORY}/${HDFS_MONTH_FOLDER} ${HDFS_GENERATED_FILE_DIRECTORY}
  fi
  
  info "Finished file ingestion"
else
  info "Skipping file ingestion"
fi

# -------------------------------------------------------------------------------------------------
# Run generate ${INTERFACE}
# -------------------------------------------------------------------------------------------------
info "Running generate files for ${INTERFACE} interface..."

nExistingScripts=`ls ${GENERATION_SCRIPT} 2> /dev/null | wc -w`

if [[ nExistingScripts -eq 1 ]]; then
  hive --hivevar op=${OP_ID}                                 \
       --hivevar op3m=${OB_3m}                               \
       --hivevar month="${CONTENT_YEAR}-${CONTENT_MONTH}-01" \
       --hivevar version=${MS_VERSION}                       \
        -f ${GENERATION_SCRIPT}
else
  die 3 "${GENERATION_SCRIPT_NAME} script not found in ${GENERATION_SCRIPT_PATH}"
fi
info "Finished generation of ${OB_3M}'s ${INTERFACE} file for ${CONTENT_DATE}"

# -------------------------------------------------------------------------------------------------
# TODO if OPTION --notify='@miqui' -> Notify @miqui
info "Finished process. SUCCESS!"
