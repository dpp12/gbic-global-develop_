#!/usr/bin/env bash
# 
# gplatform check data quality process
# ------------------------------------
# 
# Checks results of data quality model tests after executing oozie workflow.
# It has to be executed as admin user.
# It has to have read access to mysql gbic_data_quality database
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/workflow/check-data-quality.sh {country} {dataset} [{yyyyMM}]
# 
# Example:
# 
# # ${GPLATFORM_HOME}/workflow/check-data-quality.sh ES CUSTOMER 201601
# 
# Return codes:
#   >=0: Number of error tests (up to 100)
#   101: More than 100 errors on tests.
#   255: (-1) INVOCATION ERROR. Wrong argument
#   254: (-2) DEPLOYMENT ERROR. SQL script not found.
#   253: (-3) MYSQL ERROR.
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] COUNTRY DATASET [ MONTH ]"
  echo -e "Checks results of data quality tests execution looking for errors\n"
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

source ${GBIC_HOME}/common/gbic-gplatform-common.sh
source ${GBIC_HOME}/common/gbic-gplatform-env.sh

SQL_SCRIPT_PATH=${GPLATFORM_HOME}/workflow/resources
SQL_ERRORS_FILE=${SQL_SCRIPT_PATH}/dq-errors-query.sql
SQL_INFO_FILE=${SQL_SCRIPT_PATH}/dq-info-query.sql

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
        "Data quality checking script for '${OB_2M}', '${DATASET}' and ${CONTENT_DATE}"
}

# @Overrides gbic-gplatform-common.sh:cancel()
# calls showHelp() function and exits with a special CANCEL code (-1).
# Optionally, it can receive an argument with a message to print to standard output.
cancel () {
  if [[ "$1" != "" ]]; then
    echo -e "$0: $1"
  fi
  showHelp
  exit -1
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


# ARGS: OB_2M, DATASET, CONTENT_DATE (optional) Format yyyyMM
###################################################################################################
# Validate OB
# -------------------------------------------------------------------------------------------------
OB_2M=$1
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
DATASET=$2
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
MONTH=$3
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
DATASET_LOWERCASE=$(echo "${DATASET}" | awk '{print tolower($0)}')

SQL_ERRORS_QUERY="SET @gbic_op_id = ${OP_ID}; SET @filename = '${DATASET_LOWERCASE}'; SET @date = '${CONTENT_YEAR}-${CONTENT_MONTH}-01'; source ${SQL_ERRORS_FILE};"
SQL_INFO_QUERY="SET @gbic_op_id = ${OP_ID}; SET @filename = '${DATASET_LOWERCASE}'; SET @date = '${CONTENT_YEAR}-${CONTENT_MONTH}-01'; source ${SQL_INFO_FILE};"

# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched check data quality script on ${EXEC_TIME}"
debug "OPTIONS:"
debug "ARGUMENTS:"
debug "|- Country Code: '${OB_2M}'"
debug "|- Dataset to be checked: '${DATASET}'."
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date to be checked not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date to be checked: '${CONTENT_DATE}'"
fi

# -------------------------------------------------------------------------------------------------
# Validating deployment of service
# -------------------------------------------------------------------------------------------------
info "Checking service deployment..."

if [ ! -f ${SQL_ERRORS_FILE} -o ! -f ${SQL_INFO_FILE} ]; then
  die -2 "DEPLOYMENT ERROR. File with query to be executed not found in ${SQL_SCRIPT_PATH}"
fi

# -------------------------------------------------------------------------------------------------
# Checking mysql dataquality
# -------------------------------------------------------------------------------------------------
info "Executing queries in mysql dataquality database..."

info "Tests results for ${OB_2M} and ${DATASET} in ${MONTH}:"
mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} -p${MYSQL_PASS} -P${MYSQL_PORT} -e "${SQL_INFO_QUERY}" 2> .tmp-error-tests
exitCode=$?
error_msg=`cat .tmp-error-tests; rm .tmp-error-tests > /dev/null`
if [[ "${exitCode}" != "0" ]]; then
  die -3 "MYSQL ERROR. ${error_msg}"
fi

error_tests=$(mysql -N -h ${MYSQL_HOST} -u ${MYSQL_USER} -p${MYSQL_PASS} -P${MYSQL_PORT} -e "${SQL_ERRORS_QUERY}" 2> .tmp-error-tests)
exitCode=$?
error_msg=`cat .tmp-error-tests; rm .tmp-error-tests > /dev/null`
if [[ "${exitCode}" != "0" ]]; then
  die -3 "MYSQL ERROR. ${error_msg}"
fi

# If more than 100 errors, turn to 101 (because exit codes are only available up to 1 byte)
if [[ ${error_tests} -gt 100 ]]; then
  error_tests=101
fi

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. Found ${error_tests} errors."
exit ${error_tests}
