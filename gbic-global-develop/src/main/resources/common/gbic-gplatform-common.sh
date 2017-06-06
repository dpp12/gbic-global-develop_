#!/usr/bin/env bash
# 
# gbic-gplatform-common.sh
# ------------------------
# 
# General functions and constants for global platform scripts.
# It shouldn't be called directly, but included in other scripts using 'source' command.
# 
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Country codes must be always specified with 2 characters according to ISO 3166-1 alpha-2 standard
# 
# validateOB() validates a country code and returns 0 if not valid, or a positive number otherwise.
#              This number is an index for getting:
#              - the country code with 3 lowercase charactes from OBS_3m array
#              - the country code with 3 uppercase charactes from OBS_3M array
#              - the GBIC Operator ID from OBS_OP array
# getOB_3m()    returns the country code with 3 lowercase charactes for the ISO code received
# getOB_3M()    returns the country code with 3 uppercase charactes for the ISO code received
# getOpId()     returns the GBIC Operator ID for the ISO code received
# getGenFiles() returns a list of interfaces needed to be generated for the country (with genfile.sh script)
# 
# Example:
#   
#   source ../common/gbic-gplatform-common.sh
#   
#   COUNTRY=CL
#   
#   # For validating Country Code:
#   validateOB $COUNTRY; validOB=$?
#   if [[ "$validOB" = "0" ]]; then
#     cancel "wrong Country Code of file to be checked: '$COUNTRY' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_OBS_2M//[ ]/, } }."
#   fi
#   
#   # For using 3 character codes or gbic_op_id:
#   echo "Country with 2 letters (ISO 3166-1 alpha-2)...: $COUNTRY"
#   echo "Country with three letters (lowercase)........: $(getOB_3m    $COUNTRY)"
#   echo "Country with three letters (UPPERCASE)........: $(getOB_3M    $COUNTRY)"
#   echo "GBIC Operator ID..............................: $(getOpId     $COUNTRY)"
#   echo "List of interfaces to be generated............: $(getGenFiles $COUNTRY)"
# -------------------------------------------------------------------------------------------------
DEFAULT_OBS_2M="ES BR AR CL PE MX VE CO"
validateOB () {
  case $1 in
    'ES') return 1;;
    'BR') return 2;;
    'AR') return 3;;
    'CL') return 4;;
    'PE') return 5;;
    'MX') return 6;;
    'VE') return 7;;
    'CO') return 8;;
       *) return 0;;
  esac
}
OBS_3m=(--- esp bra arg chl per mex ven col)
OBS_3M=(--- ESP BRA ARG CHL PER MEX VEN COL)
OBS_OP=( -1   1 201   2   3   5   9   7   8)

GENFILES_LIST=(
    ""
    "DIM_M_BILLING_CYCLE DIM_M_GROUP_SVA"
    "DIM_M_BILLING_CYCLE DIM_M_CAMPAIGN"
    "DIM_M_CAMPAIGN DIM_M_MOVEMENT"
    "DIM_M_BILLING_CYCLE DIM_M_CAMPAIGN DIM_M_MOVEMENT DIM_M_OPERATORS"
    "DIM_M_BILLING_CYCLE DIM_M_MOVEMENT"
    ""
    ""
)

getOB_3m () {
  validateOB $1; echo ${OBS_3m[$?]}
}
getOB_3M () {
  validateOB $1; echo ${OBS_3M[$?]}
}
getOpId () {
  validateOB $1; echo ${OBS_OP[$?]}
}
getGenFiles () {
  validateOB $1; echo ${GENFILES_LIST[$?]}
}

# -------------------------------------------------------------------------------------------------
# validateDataset() validates a dataset name and returns:
# * 0 if not valid (or DEFAULT_DATASETS not initialized)
# * 1 if valid
# 
# Example:
#   
#   ../common/source gbic-gplatform-common.sh
#   
#   DATASET=CUSTOMER
#   
#   # For validating Dataset:
#   validateDataset $DATASET; validDataset=$?
#   if [[ "$validDataset" = "0" ]]; then
#     cancel "wrong Dataset name: '${dataset}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_DATASETS//[ ]/, } }."
#   fi
# -------------------------------------------------------------------------------------------------
validateDataset () {
  [[ -z ${DEFAULT_DATASETS} ]] && error "DEFAULT_DATASETS not initialized" && exit 0;
  for __dataset in ${DEFAULT_DATASETS}; do
    if [[ "$1" == "$__dataset" ]]; then
      return 1;
    fi
  done
  return 0;
}

# -------------------------------------------------------------------------------------------------
# help() calls showHelp() function and exits script with SUCCESS code (0).
# In order to use this function from a script, a showHelp() function must be implemented, showing
# the USAGE of that script.
# 
# Example:
#   
#   source gbic-gplatform-common.sh
#   
#   showHelp() {
#        echo -e "Usage: $0 [OPTION] FILE"
#        echo -e "Prints content of specified FILE\n"
#        echo -e "\t-U, --upper"
#        echo -e "\t               converts the content of file to uppercase"
#        echo -e "\t    --help     display this help and exit"
#   }
#   
#   # when executed with --help show usage and exit with no error
#   help
# -------------------------------------------------------------------------------------------------
help () {
  showHelp
  exit 0
}

# -------------------------------------------------------------------------------------------------
# cancel() calls showHelp() function and exits with CANCEL code (1).
# Optionally, it can receive an argument with a message to print to standard output.
# In order to use this function from a script, a showHelp() function must be implemented, showing
# the USAGE of that script.
# 
# Example:
#   
#   source gbic-gplatform-common.sh
#   
#   showHelp() {
#        echo -e "Usage: $0 COUNTRY"
#        echo -e "Says hello to country, using three uppercase characters\n"
#        echo -e "Recognized COUNTRY format:"
#        echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
#   }
#   
#   OB_2M=$1
#   validateOB ${OB_2M}; indexOB=$?
#   if [[ "${OB_2M}" == "" ]]; then
#     cancel "wrong Country Code of file to be checked: NOT RECEIVED"
#   elif [[ "$indexOB" = "0" ]]; then
#     cancel "wrong Country Code of file to be checked: '${OB_2M}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_OBS_2M//[ ]/, } }."
#   fi
#   
#   ob3M=$(getOB_3M ${OB_2M})
#   echo "Hello ${ob3M}!"
# -------------------------------------------------------------------------------------------------
cancel () {
  if [[ "$1" != "" ]]; then
    echo -e "$0: $1"
  fi
  showHelp
  exit 1
}

# -------------------------------------------------------------------------------------------------
# debug() info() warn() error() and fatal() print a message to standard output.
# 
# Example:
#   
#   source gbic-gplatform-common.sh
#   
#   info "Hello world!"    -- when using variables, make sure you put them into quote signs: "$3"
# -------------------------------------------------------------------------------------------------
debug () {
  message "DEBUG" "$1"
}
info () {
  message "INFO " "$1"
}
warn () {
  message "WARN " "$1"
}
error () {
  message "ERROR" "$1"
}
fatal () {
  message "FATAL" "$1"
}

# -------------------------------------------------------------------------------------------------
# message() prints a message with the specified severity.
# 
# Example:
#   
#   source gbic-gplatform-common.sh
#   
#   message "GREETING" "Hello world!"
# -------------------------------------------------------------------------------------------------
message () {
  # Log message
  echo -e "`date '+%F %H:%M:%S'` #$1 $2"
}

# -------------------------------------------------------------------------------------------------
# __die() sends an alert and ends execution.
# It receives six mandatory arguments:
# ALARM_URL...................: https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# ALARM_ICON..................: Emoji to be used in alerts. Examples ':bug:', ':bell:'
# ERROR_CODE..................: Numeric code of the error for ending the script
# ERROR_MESSAGE...............: Descriptive text of the error that caused the finalization of script
# SERVICE_NAME................: Typically taken from ${SERVICE_NAME} environment variable,
#                               it contains the name of the service ({area_name}-{project_name})
# SCRIPT_EXECUTION_DESCRIPTION: Description of the script execution. Will be inserted as text in a json document.
#                               It must contain all relevant information about error, arguments,...
#                               Example: XXXX script for ${OB_2M} and ${CONTENT_DATE}
# 
# Example:
#   
#   source ../common/gbic-gplatform-common.sh
#   source ../common/gbic-gplatform-env.sh
#   
#   ALARM_URL=${WORKFLOW_ALARM_URL}
#   
#   # Finalizes script with an error code and an alarm. It receives two arguments:
#   # - ERROR_CODE: numeric code of the error for ending the script
#   # - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
#   # Example:
#   #   die 27 "There is not connection to server"
#   die () {
#     __die "${ALARM_URL}"    \
#           ":bug:"           \
#           "$1"              \
#           "$2"              \
#           "${SERVICE_NAME}" \
#           "Test die() function script for ${OB_2M}"
#   }
#   
#   OB_2M=$1
#   
#   # some fatal error ocurred
#   die 27 "There is not connection to server"
#   
# -------------------------------------------------------------------------------------------------
__die () {
  
  __alarmUrl=$1
  __alarmIcon=$2
  __errorCode=$3
  __errorMessage=$4
  __serviceName=$5
  shift 5;
  __scriptDescription="$*"
  
  __alarmMessage="[$__serviceName] $__scriptDescription, launched by ${USER} at ${HOSTNAME}, exited with error $__errorCode.\nCause:\n$__errorMessage"
  
  if [[ $__alarmUrl != "" ]]; then
    __alarmPayload="payload={ \"text\": \"${__alarmMessage//[\"]/\\\"}\" , \"icon_emoji\": \"$__alarmIcon\"}"
    __alarmExecutionResponse=`curl -X POST --data-urlencode "$__alarmPayload" $__alarmUrl 2>/dev/null`
    info "Fired alert with response: $__alarmExecutionResponse"
  fi
  
  error "$__scriptDescription, launched by ${USER} at ${HOSTNAME}, FAILED."
  error "Cause: $__errorMessage."
  fatal "Program will exit now. Error Code $__errorCode"
  exit $__errorCode
}

# -------------------------------------------------------------------------------------------------
# Pipeline steps must be specified in GPLATFORM_PIPELINE_STEPS
# 
# validateWFStep() validates a step code and returns 0 if not valid, or a positive number otherwise.
#                  This number is an index for getting:
#                  - the step number (greater step numbers are for later steps in DAG)
# 
# Example:
#   
#   source ../common/gbic-gplatform-common.sh
#   
#   # For validating Initial Step Code:
#   validateWFStep $FROM_STEP; validStep=$?
#   if [[ "$validStep" = "0" ]]; then
#     cancel "wrong initial step: '${FROM_STEP}' NOT AVAILABLE. Please, choose one of the following: { ${GPLATFORM_PIPELINE_STEPS//[ ]/, } }."
#   fi
#   
#   # For validating Final Step Code:
#   validateWFStep $TO_STEP; validStep=$?
#   if [[ "$validStep" = "0" ]]; then
#     cancel "wrong final step: '${TO_STEP}' NOT AVAILABLE. Please, choose one of the following: { ${GPLATFORM_PIPELINE_STEPS//[ ]/, } }.""
#   fi
# -------------------------------------------------------------------------------------------------
GPLATFORM_PIPELINE_STEPS="pre ing owf"
MIN_GPLATFORM_PIPELINE_STEPS=1
MAX_GPLATFORM_PIPELINE_STEPS=`echo ${GPLATFORM_PIPELINE_STEPS} | wc -w`
validateWFStep () {
  case $1 in
    'pre') return 1;;
    'ing') return 2;;
    'owf') return 3;;
        *) return 0;;
  esac
}
