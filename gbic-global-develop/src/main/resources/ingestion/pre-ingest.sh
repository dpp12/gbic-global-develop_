#!/usr/bin/env bash
#
# gplatform inbox organization process
# ------------------------------------
#
# Takes files from { remote.inbox }/{OB}/GPLATFORM/INBOX and distributes them by date
#             into { remote.inbox }/{OB}/GPLATFORM/{VERSION}/{MONTH}.
# It has to be executed as admin user.
# It has to have write access to service's local directories, p.e: { remote.inbox }/{OB}/GPLATFORM/*
#
# Usage: (as admin) ** Execute script with --help for details on usage.
#
# # { remote.service }/ingestion/pre-ingest.sh {version} {local_path_prefix} [{country}]
#
# Example:
#
# # { remote.service }/ingestion/pre-ingest.sh MSv5 /sftp/ESP/GPLATFORM ES
#
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] VERSION LOCAL_PATH_PREFIX [COUNTRY]"
  echo -e "Takes files from local inboxes [per OB] and distributes them by date\n"
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized COUNTRY format:"
  echo -e "\tTwo character country code according to ISO 3166-1 alpha-2 standard"
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
        "File Pre-Ingestion script for ${OB_2M}"
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

# ARGS: VERSION, LOCAL_PATH_PREFIX, OB_2M (optional)
###################################################################################################
# Validate VERSION
# -------------------------------------------------------------------------------------------------
VERSION=$1
if [[ "${VERSION}" == "" ]]; then
  cancel "wrong Version of files to be organized: NOT RECEIVED"
fi
# -------------------------------------------------------------------------------------------------
# Validate INBOXPATH
# -------------------------------------------------------------------------------------------------
PREFIX=$2
if [[ "${PREFIX}" == "" ]]; then
    cancel "wrong path to move files: NOT RECEIVED"
else
    LOCAL_PATH_PREFIX=${PREFIX}
fi
# -------------------------------------------------------------------------------------------------
# Validate OB
# -------------------------------------------------------------------------------------------------
OB_2M=$3
if [[ "${OB_2M}" != "" ]]; then
  validateOB ${OB_2M}; validOB=$?
  if [[ "$validOB" = "0" ]]; then
    cancel "wrong Country Code: '${OB_2M}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_OBS_2M//[ ]/, } }."
  fi
  FILE_OB_PREFIX="${OB_2M}_"
else
  FILE_OB_PREFIX=${DEFAULT_FILE_OB_PREFIX}
fi

# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`

CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}

PROCESSING_FILE=PROCESSING
SUCCESS_FILE=SUCCESS
ERROR_FILE=ERROR

LOCAL_INBOX_PATH="${LOCAL_PATH_PREFIX}/INBOX"
SERVICE_VERSION_PATH="${LOCAL_PATH_PREFIX}/${VERSION}"
TEMP_LOCAL_PATH="${LOCAL_INBOX_PATH}/TEMP"

INTERFACE_DESCRIPTOR="INTERFACE_DESCRIPTOR"


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched pre ingestion script for ${VERSION} on ${EXEC_TIME}"
debug "OPTIONS:"
debug "ARGUMENTS:"
debug "|- Version of files to be organized: '${VERSION}'"
debug "|- Files will be moved from '${LOCAL_INBOX_PATH}' to '${SERVICE_VERSION_PATH}/{date}'"
if [[ "${OB_2M}" == "" ]]; then
  # Console message
  debug "|- Country code of files to be pre-ingested not specified. Empty OB will be used"
else
  # Console message
  debug "|- Country code of files to be pre ingested: '${OB_2M}'"
fi

mkdir -p ${TEMP_LOCAL_PATH}

cd ${LOCAL_INBOX_PATH}

# 1. Re-Run (partially processed interface recovery)
#    Converting PROCESSING file into a new interface-descriptor (type CONTINUE)
# -----------------------------------------------------------------------------
if [ -f ${PROCESSING_FILE} ]; then
  info "Process Re-Execution. Creating new Interface Descriptor for CONTINUING process"
  nuevo_ifd=${FILE_OB_PREFIX}${INTERFACE_DESCRIPTOR}_`date '+%Y%m%d%H%M%S%N'`_CONTINUE.txt
  if [ -f ${SUCCESS_FILE} ]; then
    info "There's a ${SUCCESS_FILE}. Removing already processed files from new Interface Descriptor"
    grep -v -f ${SUCCESS_FILE} ${PROCESSING_FILE} > ${nuevo_ifd}
  else
    mv ${PROCESSING_FILE} ${nuevo_ifd}
  fi
  rm -fr ${PROCESSING_FILE}
fi

# 2. Converting ERROR file into a new interface-descriptor (type FIX)
# -----------------------------------------------------------------------------
if [ -f ${ERROR_FILE} ]; then
  info "Adding files that could not be precessed in the past to the list of files to process..."
  mv ${ERROR_FILE} ${FILE_OB_PREFIX}${INTERFACE_DESCRIPTOR}_`date '+%Y%m%d%H%M%S%N'`_FIX.txt
fi

# 3. Processing new files
# -----------------------------------------------------------------------------
# merge interface descriptors
info "Merging Interface Descriptor files..."
if [ -f ${SUCCESS_FILE} ]; then
  mv ${SUCCESS_FILE} ${SUCCESS_FILE}.tmp
else
  touch ${SUCCESS_FILE}.tmp
fi
IFDS=`ls ${FILE_OB_PREFIX}${INTERFACE_DESCRIPTOR}_*.txt 2>/dev/null`

file_error=0
if [[ "${IFDS}" != "" ]]; then
  
  rm -fr ${PROCESSING_FILE}.tmp
  for ifd in ${IFDS}; do
    # processing files (removing BOM character)
    sed -e '1s/^\xef\xbb\xbf//' ${ifd} >> ${PROCESSING_FILE}.tmp
    mv ${ifd} ${ifd}.DONE
  done
  sort -u ${PROCESSING_FILE}.tmp > ${PROCESSING_FILE}.tmp2
  grep -v -f ${PROCESSING_FILE}.tmp2 ${SUCCESS_FILE}.tmp > ${SUCCESS_FILE}.tmp2
  
  sort -u ${SUCCESS_FILE}.tmp2 > ${SUCCESS_FILE}
  mv ${PROCESSING_FILE}.tmp2 ${PROCESSING_FILE}
  
  rm -fr ${PROCESSING_FILE}.tmp*
  rm -fr ${SUCCESS_FILE}.tmp*
  
  # processing files
  FILES=`cat ${PROCESSING_FILE}`
  # processing files (removing ^M character)
  FILES=`echo -e "${FILES}" | sed -e 's/\r//g'`
  
  for file in ${FILES}; do
    if [ -f ${file} ]; then
      
      info "processing ${file}..."
      
      # ---------------------------------------------------------------------------------
      info "Checking file integrity for ${file}..."
      file_error=0
      file_extension=`echo ${file##*.} | tr '[:upper:]' '[:lower:]'`
      
      if [[ "${file_extension}" == "bz2" ]]; then
        bzip2 --test ${file} 2> /dev/null
        file_error=$?
      else
        warn "Unable to check file integrity for ${file_extension} files"
      fi
      
      if [[ "${file_error}" != "0" ]]; then
        error "Incomplete or Corrupt file: ${file}"
        echo ${file} >> ${ERROR_FILE}
      else
        
        date=`echo ${file} | sed -e 's/_QUALITY//g'`
        date=$(echo ${date} | awk -F_ '{ print $NF }' | awk -F. '{ print $1 }')
        mkdir -p ${SERVICE_VERSION_PATH}/${date}
        
        info "Moving file ${file} to ${SERVICE_VERSION_PATH}/${date}..."
        mv ${file} ${SERVICE_VERSION_PATH}/${date}/.
        echo ${file} >> ${SUCCESS_FILE}
        
      fi
      # ---------------------------------------------------------------------------------
      
    else
      echo ${file} >> ${ERROR_FILE}
    fi
  done
  rm -fr PROCESSING
else
  info "There's no file to process in INBOX path '${LOCAL_INBOX_PATH}'"
fi
cd - > /dev/null

if [[ "${file_error}" != "0" ]]; then
  die 2 "Some error(s) occurred. See ${ERROR_FILE} for details on list of unprocessed files. Maybe some file was not completely copied. You can try again or check file integrity manually"
fi

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
