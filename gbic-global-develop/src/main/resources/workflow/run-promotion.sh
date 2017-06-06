#!/usr/bin/env bash
# 
# gplatform run promotion process
# ------------------------------------
# 
# Launch promotion process to move data from staging to gold zone.
# It has to be executed as admin user.
# It has to have write access to Hive gbic_global database.
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/workflow/run-promotion.sh {vers-num} {country} {dataset} [{yyyyMM}]
# 
# Example:
# 
# # ${GPLATFORM_HOME}/workflow/run-promotion.sh 5 ES CUSTOMER 201601
# 
# Return codes:
#  0: Success
#  1: INVOCATION ERROR. Wrong argument
#  2: DEPLOYMENT ERROR. HQL script not found.
#  3: HIVE ERROR.
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] MS_VERSION COUNTRY DATASET [ MONTH ]"
  echo -e "Run promotion process to move data from staging to gold zone\n"
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized COUNTRY format:"
  echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
  echo -e "\nRecognized DATASET format:"
  echo -e "\tUppercase dataset name"
  echo -e "\nRecognized MONTH format:"
  echo -e "\tYYYYMM"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-env.sh
source ${GBIC_HOME}/common/gbic-gplatform-common.sh

PROMOTE_SCRIPT_PREFIX=${GPLATFORM_HOME}/etl/scripts
ERROR_COMMAND_LINE='Command failed with exit code = *'

ALARM_URL=${WORKFLOW_ALARM_URL}

# Finalizes script with an error code and an alarm. It receives two arguments:
# - ERROR_CODE: numeric code of the error for ending the script
# - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
# Example:
#   die 27 "There is not connection to server"
die () {
  __die "${ALARM_URL}"    \
        ":bug:"           \
        "$1"              \
        "$2"              \
        "${SERVICE_NAME}" \
        "Run promotion script for '${OB_2M}', '${DATASET}' and ${CONTENT_DATE}"
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


# ARGS: MS_VERSION, OB_2M, DATASET, CONTENT_DATE (optional) Format yyyyMM
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
if [[ "${OB_2M}" == "" ]]; then
  cancel "wrong Country: NOT RECEIVED"
else
  validateOB ${OB_2M}; validOB=$?
  if [[ "$validOB" = "0" ]]; then
    cancel "wrong Country Code: '${OB_2M}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_OBS_2M//[ ]/, } }."
  fi
fi
# -------------------------------------------------------------------------------------------------
# Validate DATASET
# -------------------------------------------------------------------------------------------------
DATASET=$3
if [[ "$DATASET" == "" ]]; then
  cancel "wrong Dataset name: NOT RECEIVED"
else
  validateDataset ${DATASET}; validDataset=$?
  if [[ "$validDataset" = "0" ]]; then
    cancel "wrong Dataset name: '${dataset}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_DATASETS//[ ]/, } }."
  fi
fi
# -------------------------------------------------------------------------------------------------
# Validate MONTH
# -------------------------------------------------------------------------------------------------
MONTH=$4
if [[ "${MONTH}" == "" ]]; then
  LOAD_DATE=`date '+%Y%m'`
  CONTENT_DATE=$(date "--date=${LOAD_DATE}01 -1 month" +"%Y%m")
else
  CONTENT_DATE=${MONTH}
fi


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`

CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}

OP_ID=$(getOpId ${OB_2M})
OB_3m=$(getOB_3m ${OB_2M})
DATASET_LOWERCASE=$(echo "${DATASET}" | awk '{print tolower($0)}')
PROMOTE_SCRIPT_PATH=${PROMOTE_SCRIPT_PREFIX}/${DATASET_LOWERCASE}
PROMOTE_SCRIPT_FILE=${PROMOTE_SCRIPT_PATH}/${DATASET_LOWERCASE}HivePromotion.hql

if [[ -z ${HADOOP_TOKEN_FILE_LOCATION} ]]; then
  HIVE_KERBEROS="SET mapreduce.job.credentials.binary=${HADOOP_TOKEN_FILE_LOCATION}; "
else
  HIVE_KERBEROS=""
fi

HIVE_QUERY="${HIVE_KERBEROS}SET hivevar:ob=${OB_3m}; SET hivevar:version=MSv${MS_VERSION}; SET hivevar:nominalTime=${CONTENT_YEAR}-${CONTENT_MONTH}-01; SET hivevar:gbic_op_id=${OP_ID}; SET hivevar:fileName=${DATASET_LOWERCASE}; source ${PROMOTE_SCRIPT_FILE};"


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched promotion script on ${EXEC_TIME}"
debug "OPTIONS:"
debug "ARGUMENTS:"
debug "|- Semantic Model Version of files to be promoted: '${MS_VERSION}' (MSv${MS_VERSION})"
debug "|- Country Code: '${OB_2M}'"
debug "|- Dataset to be promoted: '${DATASET}'."
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date to be promoted not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date to be promoted: '${CONTENT_DATE}'"
fi

# -------------------------------------------------------------------------------------------------
# Validating deployment of service
# -------------------------------------------------------------------------------------------------
info "Checking service deployment..."

if [ ! -f ${PROMOTE_SCRIPT_FILE} ]; then
  die 2 "DEPLOYMENT ERROR. File with query to be executed not found in ${PROMOTE_SCRIPT_PATH}"
fi

# -------------------------------------------------------------------------------------------------
# Launch promot script
# -------------------------------------------------------------------------------------------------
info "Launch promotion script..."

hive -e "${HIVE_QUERY}" --hiveconf hive.execution.engine=mr > .tmp-error-tests 2>&1
exitCode=$?
error_msg=`tail -2 .tmp-error-tests; rm .tmp-error-tests > /dev/null 2>&1`
if [[ "${exitCode}" != "0" ]]; then
  # Retrieve error message from hive -------------------
  LAST_LINE=`echo -e "${error_msg}" | tail -1`
  if [[ ${LAST_LINE} == ${ERROR_COMMAND_LINE} ]]; then
    # Get error from hdfs command execution
    error_msg=`echo -e "${error_msg}" | head -1`
  else
    # Get error from hive execution
    error_msg=`echo -e "${error_msg}" | tail -1`
  fi
  # ----------------------------------------------------
  die 3 "HIVE ERROR. ${error_msg}"
fi

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
