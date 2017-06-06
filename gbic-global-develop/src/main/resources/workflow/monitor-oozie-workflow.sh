#!/usr/bin/env bash
# 
# gplatform monitor oozie workflow process
# ----------------------------------------
# 
# Monitor the execution of an Oozie coordinator, waiting for it to finished and returning the status of the termination.
# 
# It has to be executed as admin user.
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/workflow/monitor-oozie-workflow.sh {job-id}
# 
# Example:
# 
# # ${GPLATFORM_HOME}/workflow/monitor-oozie-workflow.sh -f job_status.txt xxxxxxx-xxxxxxxxxxxxxxx-oozie-oozi-C
#
# Return codes:
#  0: Oozie coordinator finished with status SUCCEEDED
#  1: Invocation ERROR. Wrong argument
#  4: Oozie coordinator finished with status SUSPEND
#  5: Oozie coordinator finished with status FAILED
#  6: Oozie coordinator finished with status KILLED
#  7: Oozie coordinator finished with status unknown
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] JOB_ID"
  echo -e "Monitor the execution of an Oozie coordinator and returns the status of termination\n"
  echo -e "\t-r SECONDS, --refresh=SECONDS"
  echo -e "\t               seconds for the loop to sleep between oozie job status info executions."
  echo -e "\t               If not present, it will sleep 60 seconds by default."
  echo -e "\t-f FILE, --jobstatus-file=FILE"
  echo -e "\t               absolute path of the file in which job end status must be written."
  echo -e "\t               If not present, no file will be written."
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized JOBID format:"
  echo -e "\txxxxxxx-xxxxxxxxxxxxxxx-oozie-oozi-C"
  echo -e "\nReturn codes (errors & coordinator termination status):"
  echo -e "\t  0: Oozie coordinator finished with status SUCCEEDED"
  echo -e "\t  1: Invocation ERROR. Wrong argument"
  echo -e "\t  4: Oozie coordinator finished with status SUSPEND"
  echo -e "\t  5: Oozie coordinator finished with status FAILED"
  echo -e "\t  6: Oozie coordinator finished with status KILLED"
  echo -e "\t  7: Oozie coordinator finished with status unknown"
  echo -e "\t"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-env.sh
source ${GBIC_HOME}/common/gbic-gplatform-common.sh

DEFAULT_REFRESH=60
DEFAULT_JOBSTATUS_FILE=.tmp_status.txt

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
        "Oozie workflow monitor script for oozie job ${JOB_ID}"
}

# checkJobStatus() monitor and get code for the different Oozie status:
#  0: Oozie coordinator status SUCCEEDED
#  2: Oozie coordinator status PREP
#  3: Oozie coordinator status RUNNING
#  4: Oozie coordinator status SUSPEND
#  5: Oozie coordinator status FAILED
#  6: Oozie coordinator status KILLED
#  7: Oozie coordinator status unknown
# -1: Exception
# Example:
#   checkJobStatus $JOB_ID $ERROR_FILE; oozieCode=$?
checkJobStatus () {
  __jobId=$1
  __errFile=$2
  
  status=`oozie job -info ${__jobId}@1 -oozie ${OOZIE_URL} 2> ${__errFile} | grep '^Status' | tr -d ' ' | awk -F':' '{ print $2}'`
  if [[ "${status}" != "" ]]; then
    echo ${status} > ${__errFile}
  fi
  rc=
  case $status in
         "PREP") rc=2;;  # Not ended
      "RUNNING") rc=3;;  # Not ended
      "SUSPEND") rc=4;;  # Not ended
    "SUCCEEDED") rc=0;;  # OK
             "") rc=-1;; # FATAL (Exception)
       "FAILED") rc=5;;  # ERROR job failed
       "KILLED") rc=6;;  # ERROR job was killed
              *) rc=7;;  # ERROR (unknown)
  esac
  return ${rc}
}

# OPTIONS: -r SECONDS, -f FILE
###################################################################################################
OPT_REFRESH=
OPT_JOBSTATUS_FILE=

while getopts '\-:-r:-f:' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        refresh=?*        ) OPT_REFRESH=${long_optarg};;
        refresh*          ) cancel "option requires an argument -- ${OPTARG}";;
        jobstatus-file=?* ) OPT_JOBSTATUS_FILE=${long_optarg};;
        jobstatus-file*   ) cancel "option requires an argument -- ${OPTARG}";;
        help              ) help;;
        # "--" terminates argument processing
        ''                ) break;;
        *                 ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
    r ) OPT_REFRESH=${OPTARG};;
    f ) OPT_JOBSTATUS_FILE=${OPTARG};;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# -------------------------------------------------------------------------------------------------
# Validate REFRESH
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_REFRESH}" == "" ]]; then
  REFRESH=${DEFAULT_REFRESH}
else
  REFRESH=${OPT_REFRESH}
fi
# -------------------------------------------------------------------------------------------------
# Validate JOBSTATUS_FILE
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_JOBSTATUS_FILE}" == "" ]]; then
  JOBSTATUS_FILE=${DEFAULT_JOBSTATUS_FILE}
else
  JOBSTATUS_FILE=${OPT_JOBSTATUS_FILE}
fi


# ARGS: JOB_ID
###################################################################################################
# Validate JOB_ID
# -------------------------------------------------------------------------------------------------
JOB_ID=$1
if [[ "${JOB_ID}" == "" ]]; then
  cancel "wrong Job ID of the Oozie coordinator to be monitorized: NOT RECEIVED"
fi


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched monitor oozie workflow script on ${EXEC_TIME}"
debug "OPTIONS:"
if [[ "${OPT_REFRESH}" == "" ]]; then
  debug "|- Refresh option not present. Default refresh will be used (${DEFAULT_REFRESH} seconds)"
else
  debug "|- Refresh option present: ${REFRESH} seconds"
fi
if [[ "${OPT_JOBSTATUS_FILE}" == "" ]]; then
  debug "|- Job status file option not present. No file will be written."
else
  debug "|- Job status file present: ${JOBSTATUS_FILE}"
fi
debug "ARGUMENTS:"
debug "|- Job ID of the Oozie coordinator to be monitorized: '${JOB_ID}'"

# -------------------------------------------------------------------------------------------------
# Coordinator monitoring
# -------------------------------------------------------------------------------------------------
info "Monitor launched oozie workflow (every ${REFRESH} seconds)"

checkJobStatus ${JOB_ID} ${JOBSTATUS_FILE}; returnCode=$?
while [[ "${returnCode}" == "2" || "${returnCode}" == "3" ]]; do
  sleep ${REFRESH}
  checkJobStatus ${JOB_ID} ${JOBSTATUS_FILE}; returnCode=$?
done

OOZIE_STATUS=`cat ${JOBSTATUS_FILE}`
#If no jobstatus_file was asked, remove file
if [[ "${OPT_JOBSTATUS_FILE}" == "" ]]; then
  rm ${JOBSTATUS_FILE}
fi

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. Oozie job finished with status: ${OOZIE_STATUS}!"
exit ${returnCode}
