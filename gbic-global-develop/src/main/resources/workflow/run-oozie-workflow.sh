#!/usr/bin/env bash
# 
# gplatform run oozie workflow process
# ------------------------------------
# 
# Creates a copy of the .properties template filling the values with the information given and run oozie workflow.
# If oozie workflow has been launched successfuly and jobid_file option was given, writes in these file
# the id of the oozie job launched.
# 
# It has to be executed as admin user.
# It has to have write access to local properties directory, p.e: ${GPLATFORM_HOME}/etl/oozie/config/
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/workflow/run-oozie-workflow.sh {vers-num} {country} {dataset-list} [{yyyyMM}]
# 
# Example:
# 
# # ${GPLATFORM_HOME}/workflow/run-oozie-workflow.sh -f jobid.txt 5 ES "CUSTOMER MOVEMENTS" 201601
#
# Return codes:
#  0: Success
#  1: INVOCATION ERROR. Wrong argument
#  2: DEPLOYMENT ERROR. Properties template file not found.
#  3: OPERATION ERROR. Target path cannot be created.
#  4: SYNTAX ERROR. Template with wrong syntax.
#  5: OPERATION ERROR. Properties file cannot be created.
#  6: OOZIE ERROR. Failed to launch oozie job.
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] MS_VERSION COUNTRY DATASET_LIST [ MONTH ]"
  echo -e "Fills a copy of the properties template with the information given as arguments and run oozie workflow\n"
  echo -e "\t-q, --skip-dq"
  echo -e "\t               execute workflow skipping data quality processes."
  echo -e "\t               It won't be enabled by default (so DQ will run)."
  echo -e "\t-l, --skip-local"
  echo -e "\t               execute workflow skipping local screens execution."
  echo -e "\t               It won't be enabled by default (so local screens will run)."
  echo -e "\t-g, --skip-global"
  echo -e "\t               execute workflow skipping global screens execution."
  echo -e "\t               It won't be enabled by default (so global screens will run)."
  echo -e "\t-p, --auto-promote"
  echo -e "\t               data will be automatically advanced to the gold zone at the end of the workflow."
  echo -e "\t               It won't be enabled by default (so data will remain in staging zone)."
  echo -e "\t-f FILE, --jobid-file=FILE"
  echo -e "\t               absolute path of the file in which job id must be written."
  echo -e "\t               If not present, no file will be written."
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized COUNTRY format:"
  echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
  echo -e "\nRecognized DATASET_LIST format:"
  echo -e "\tSpace separated list of uppercase datasets names"
  echo -e "\nRecognized MONTH format:"
  echo -e "\tYYYYMM"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-env.sh
source ${GBIC_HOME}/common/gbic-gplatform-common.sh

DEFAULT_JOBID_FILE=

TEMPLATE_PATH=${GPLATFORM_HOME}/workflow/resources
TEMPLATE_FILE=${TEMPLATE_PATH}/coordinator-template.properties
TEMP_PATH=${GPLATFORM_HOME}/etl/oozie/config/temp

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
        "Oozie workflow execution script for '${OB_2M}', '${DATASETS}' and ${CONTENT_DATE}"
}


# OPTIONS: -q, -l, -g, -p, -f FILE
###################################################################################################
SKIP_DQ=0
SKIP_LOCAL=0
SKIP_GLOBAL=0
AUTO_PROMOTE=0
OPT_JOBID_FILE=

while getopts '\-:-f:qlgp' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        skip-dq        ) SKIP_DQ=1;;
        skip-dq*       ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        skip-local     ) SKIP_LOCAL=1;;
        skip-local*    ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        skip-global    ) SKIP_GLOBAL=1;;
        skip-global*   ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        auto-promote   ) AUTO_PROMOTE=1;;
        auto-promote*  ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        jobid-file=?*  ) OPT_JOBID_FILE=${long_optarg};;
        jobid-file*    ) cancel "option requires an argument -- ${OPTARG}";;
        help           ) help;;
        # "--" terminates argument processing
        ''             ) break;;
        *              ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
    q ) SKIP_DQ=1;;
    l ) SKIP_LOCAL=1;;
    g ) SKIP_GLOBAL=1;;
    p ) AUTO_PROMOTE=1;;
    f ) OPT_JOBID_FILE=${OPTARG};;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# -------------------------------------------------------------------------------------------------
# Validate JOBID_FILE
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_JOBID_FILE}" == "" ]]; then
  JOBID_FILE=${DEFAULT_JOBID_FILE}
else
  JOBID_FILE=${OPT_JOBID_FILE}
fi


# ARGS: MS_VERSION, OB_2M, DATASETS, CONTENT_DATE (optional) Format yyyyMM
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
# Validate DATASETS
# -------------------------------------------------------------------------------------------------
DATASETS=$3
if [[ "${DATASETS}" == "" ]]; then
  cancel "wrong Dataset list: NOT RECEIVED"
else
  for dataset in ${DATASETS}; do
    validateDataset ${dataset}; validDataset=$?
    if [[ "$validDataset" = "0" ]]; then
      cancel "wrong Dataset name: '${dataset}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_DATASETS//[ ]/, } }."
    fi
  done
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


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`

CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}

PROPERTIES_FILE=${TEMP_PATH}/coordinator-${OB_2M}-${EXEC_TIME/ /_}.properties

# Initialize all datasets setting them to 0
datasets_init=$(for dataset in ${DEFAULT_DATASETS}; do
  echo "${dataset}=0;"
done)
eval ${datasets_init}

# -------------------------------------------------------------------------------------------------
# Template variables
# -------------------------------------------------------------------------------------------------
# START_DATE: +1 month from the given date
START_DATE=$(date "--date=${CONTENT_DATE}01 +1 month" +"%Y-%m-")01
# END_DATE: +2 months from the given date
END_DATE=$(date "--date=${CONTENT_DATE}01 +2 month" +"%Y-%m-")01

OB_3m=$(getOB_3m ${OB_2M})
OP_ID=$(getOpId ${OB_2M})

# DATASET activation: those that appear in argument "DATASETS" are setted to 1
datasets_activation=$(for dataset in ${DATASETS}; do
  echo "${dataset}=1;"
done)
eval ${datasets_activation}


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched run oozie workflow script on ${EXEC_TIME}"
debug "OPTIONS:"
debug "|- Data Quality:"
if [[ "${SKIP_DQ}" == "0" ]]; then
  if [[ "${SKIP_GLOBAL}" == "0" ]]; then
    debug "|  > Skip Global DQ flag not setted. Global screens will be executed"
  else
    debug "|  > Skip Global DQ flag setted. No global screens will be executed"
  fi
  if [[ "${SKIP_LOCAL}" == "0" ]]; then
    debug "|  > Skip Local DQ flag not setted. Local screens will be executed"
  else
    debug "|  > Skip Local DQ flag setted. No local screens will be executed"
  fi
else
  debug "|  > Skip Data Quality flag setted. No Data quality model will be executed"
fi
if [[ "${AUTO_PROMOTE}" == "0" ]]; then
  debug "|- Auto-Promote Data flag not setted. Data will NOT be automatically advanced to the gold zone"
else
  debug "|- Auto-Promote Data flag setted. Data will be automatically advanced to the gold zone"
fi
if [[ "${OPT_JOBID_FILE}" == "" ]]; then
  debug "|- Job id file option not present. No file will be written."
else
  debug "|- Job id file present: ${JOBID_FILE}"
fi
debug "ARGUMENTS:"
debug "|- Semantic Model Version of files to be ingested: '${MS_VERSION}' (MSv${MS_VERSION})"
debug "|- Country Code: '${OB_2M}'"
debug "|- Datasets to be loaded: '${DATASETS}'."
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date of files to be loaded not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date of file to be loaded: '${CONTENT_DATE}'"
fi

# -------------------------------------------------------------------------------------------------
# Template reading
# -------------------------------------------------------------------------------------------------
info "Reading template..."
foundTemplate=
if [ -f ${TEMPLATE_FILE} ]; then
  info "Found properties template"
  foundTemplate=${TEMPLATE_FILE}
fi
if [[ "${foundTemplate}" != "" ]]; then
  
  substScript=$(while read -r line; do
    echo "echo $line;"
  done < ${foundTemplate})
  
else
  die 2 "Template not found in ${TEMPLATE_PATH}"
fi

# -------------------------------------------------------------------------------------------------
# File writing preparation
# -------------------------------------------------------------------------------------------------
info "Ensuring existence of directory '${TEMP_PATH}'"
mkdir -p ${TEMP_PATH}
exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 3 "Target directory could not be created '${TEMP_PATH}'. Please, see log for details."
fi

# -------------------------------------------------------------------------------------------------
# Template syntax validation
# -------------------------------------------------------------------------------------------------
info "File ${PROPERTIES_FILE} will be generated with following content:"
echo;
eval $substScript
exitCode=$?
echo;
if [[ "${exitCode}" != "0" ]]; then
  die 4 "Syntax error on template file: ${foundTemplate}. Please, see log for details."
fi

# -------------------------------------------------------------------------------------------------
# PROPERTIES file writing
# -------------------------------------------------------------------------------------------------
info "Creating ${PROPERTIES_FILE}"
eval $substScript > ${PROPERTIES_FILE}
exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 5 "File could not be created. Please, see log for details."
fi
info "Properties file created from template: ${TEMPLATE_FILE}"

# -------------------------------------------------------------------------------------------------
# Launching oozie job
# -------------------------------------------------------------------------------------------------
job_id=$(oozie job -run -config ${PROPERTIES_FILE} -oozie ${OOZIE_URL} 2> .tmp-error-tests)
exitCode=$?
error_msg=`cat .tmp-error-tests; rm .tmp-error-tests > /dev/null`

if [[ ${exitCode} -ne 0 ]]; then
  die 6 "Failed to launch Oozie Job: ${error_msg}"
fi
info "Launched oozie workflow. Job id: ${job_id}"

# -------------------------------------------------------------------------------------------------
# Write job_id file
# -------------------------------------------------------------------------------------------------
if [[ "JOBID_FILE" != "" ]]; then
  info "Write new job id into file: ${JOBID_FILE}"
  echo ${job_id} | awk -F':' '{ print $2}' | tr -d ' ' > ${JOBID_FILE}
fi

# -------------------------------------------------------------------------------------------------
# Removing properties file
# -------------------------------------------------------------------------------------------------
rm ${PROPERTIES_FILE}
info "Removed properties file: ${PROPERTIES_FILE}"

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
