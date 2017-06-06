#!/usr/bin/env bash
# 
# gplatform export files process
# ------------------------------
# 
# Copy raw files from gplatform to export directory.
# It has to be executed as admin user.
# It has to have write access to export directory, p.e: { remote.inbox }/LTV/GBICtoLTV/{ country }/GPLATFORM/MSv{ vers-num }/{ yyyyMM }
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/workflow/export-files.sh {vers-num} {country} { export-path } [{yyyyMM}] >> {logfile}
# 
# Example:
# 
# # ${GPLATFORM_HOME}/workflow/export-files.sh 5 ES /sftp/LTV/GBICtoLTV 201601 >> /var/log/industrializacion/calidad_servicio/copyFiles.log
#
# Return codes:
#  0: Success
#  1: INVOCATION ERROR. Wrong call
#  2: OPERATION ERROR. Could not create destination path.
#  3: OPERATION ERROR. Source path does not exist.
#  4: OPERATION ERROR. Could not copy files from source to path.
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] MS_VERSION COUNTRY EXPORT_PATH [ MONTH ]"
  echo -e "Copy raw files from gplatform inbox to export directory\n"
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized COUNTRY format:"
  echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
  echo -e "\nRecognized MONTH format:"
  echo -e "\tYYYYMM"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-env.sh
source ${GBIC_HOME}/common/gbic-gplatform-common.sh

ALARM_URL=${EXPORT_ALARM_URL}

# Finalizes script with an error code and an alarm. It receives two arguments:
# - ERROR_CODE: numeric code of the error for ending the script
# - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
# Example:
#   die 27 "There is not connection to server"
die () {
  __die "${ALARM_URL}"    \
        ":arrow_down:"    \
        "$1"              \
        "$2"              \
        "${SERVICE_NAME}" \
        "Export raw files script for ${OB_2M} and ${CONTENT_DATE} to ${EXPORT_PATH}"
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


# ARGS: MS_VERSION, OB_2M, EXPORT_PATH, CONTENT_DATE (optional) Format yyyyMM
###################################################################################################
# Validate MS_VERSION
# -------------------------------------------------------------------------------------------------
MS_VERSION=$1
if [[ "${MS_VERSION}" == "" ]]; then
  cancel "wrong Semantic Model Version of file to be copied: NOT RECEIVED"
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
# Validate EXPORT_PATH
# -------------------------------------------------------------------------------------------------
EXPORT_PATH=$3
if [[ "${EXPORT_PATH}" == "" ]]; then
  cancel "wrong Export path where files must be copied: NOT RECEIVED"
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

OB_3M=$(getOB_3M ${OB_2M})

SOURCE_PATH=${DEFAULT_LOCAL_PATH_PREFIX}/${OB_3M}/GPLATFORM/MSv${MS_VERSION}/${CONTENT_DATE}
DEST_PATH=${EXPORT_PATH}/${OB_3M}/GPLATFORM/MSv${MS_VERSION}


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched export script on ${EXEC_TIME}"
debug "OPTIONS:"
debug "ARGUMENTS:"
debug "|- Semantic Model Version of files to be copied: '${MS_VERSION}' (MSv${MS_VERSION})"
debug "|- Country Code: '${OB_2M}'"
debug "|- Export Path: '${EXPORT_PATH}'"
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date to be copied not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date to be copied: '${CONTENT_DATE}'"
fi

# -------------------------------------------------------------------------------------------------
# Desination path existence checking
# -------------------------------------------------------------------------------------------------
info "Checking existence of destination path ${DEST_PATH}..."

mkdir -p ${DEST_PATH}
exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 2 "OPERATION ERROR. Could not create destination path ${DEST_PATH}"
fi

# -------------------------------------------------------------------------------------------------
# Source path existence checking
# -------------------------------------------------------------------------------------------------
info "Checking existence of source path ${SOURCE_PATH}..."

if [[ ! -d "${SOURCE_PATH}" ]]; then
  die 3 "OPERATION ERROR. Source path ${SOURCE_PATH} to be copied does not exist"
fi

# -------------------------------------------------------------------------------------------------
# Source path existence checking
# -------------------------------------------------------------------------------------------------
info "Copying files from ${SOURCE_PATH} to ${DEST_PATH}..."

cp -r ${SOURCE_PATH} ${DEST_PATH}
exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 4 "OPERATION ERROR. Could not copy files from ${SOURCE_PATH} to ${DEST_PATH}. Error: ${error_msg}"
else
  touch ${DEST_PATH}/${CONTENT_DATE}/_SUCCESS
fi

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
