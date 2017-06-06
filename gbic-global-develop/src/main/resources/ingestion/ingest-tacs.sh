#!/usr/bin/env bash
# 
# gplatform ingestion devices catalog process
# --------------------------------------------
# 
# Takes files from ${LOCAL_INBOX} and distributes them into HDFS
# It has to be executed as admin user.
# It has to have write access to local inbox directory: { inbox }/LTV/GBICtoLTV
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/ingestion/ingest-tacs.sh [{yyyyMM} [{local_path_prefix}]]
# 
# Example:
# 
# # ${GPLATFORM_HOME}/ingestion/ingest-files.sh 201501
# 
# Return codes:
#  0: Success
#  1: INVOCATION ERROR. Wrong call
#  2: OPERATION ERROR. File not found.
#  3: OPERATION ERROR. Backup failed.
#  4: OPERATION ERROR. Incomplete or Corrupt file.
#  5: OPERATION ERROR. Failed to descompress data file.
#  6: OPERATION ERROR. Failed to convert data file.
#  7: OPERATION ERROR. Failed to put data file to HDFS.
#
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] [ MONTH [LOCAL_PATH_PREFIX] ]"
  echo -e "Takes devices catalogue from local inbox and puts it into HDFS\n"
  echo -e "\t    --help     display this help and exit"
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
        "Devices Catalog ingestion script for ${CONTENT_DATE}"
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

# ARGS: CONTENT_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
###################################################################################################
MONTH=$1
if [[ "${MONTH}" == "" ]]; then
  LOAD_DATE=`date '+%Y%m'`
  CONTENT_DATE=$(date "--date=${LOAD_DATE}01 -1 month" +"%Y%m")
else
  CONTENT_DATE=${MONTH}
fi
# -------------------------------------------------------------------------------------------------
# Validate LOCAL_PATH_PREFIX
# -------------------------------------------------------------------------------------------------
PREFIX=$2
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

LOCAL_INBOX="LTV/LTVtoGBIC"
DEVICES_CATALOG="DEVICES_CATALOG"
FILE_NAME="SF_ExportCatalogo_GBI"

TEMP_LOCAL_PATH="TEMP"
LOGFILE_NAME=log-ingest-files-${EXEC_TIME//[ ]/_}.log
LOGFILE_NAME=${LOGFILE_NAME//[:]/-}
LOGFILE_HDFS_PATH=${HDFS_TACS}/_ingestion-logs
LOGFILE_LOCAL_PATH=${LOCAL_PATHS_PREFIX}/${LOCAL_INBOX}/${TEMP_LOCAL_PATH}
LOG_FILE=${LOGFILE_LOCAL_PATH}/${LOGFILE_NAME}

# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched ingestion-tacs script on ${EXEC_TIME}"
debug "Logs will be available on ${LOG_FILE}"
debug "OPTIONS:"
debug "ARGUMENTS:"
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date of Devices Catalog file to be ingested not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date of Devices Catalog file to be ingested: '${CONTENT_DATE}'"
fi
if [[ "${PREFIX}" == "" ]]; then
  # Console message
  debug "|- Root folder for Devices Catalog file to be ingested not specified. Default will be used: '${LOCAL_PATHS_PREFIX}'"
else
  # Console message
  debug "|- Root folder for Devices Catalog file to be ingested: '${LOCAL_PATHS_PREFIX}'"
fi

gzip_file_path=${LOCAL_PATHS_PREFIX}/${LOCAL_INBOX}
file_error=0

# Console message
debug "logs: tail -F \`ls -rt ${LOGFILE_LOCAL_PATH}/log-ingest-files-*_*.log | tail -1\`"

mkdir -p ${LOGFILE_LOCAL_PATH}

# Log message
info "Searching files for ${CONTENT_DATE} in ${gzip_file_path}. Launched at $EXEC_TIME" > ${LOG_FILE} 2>&1

HDFS_PATH="${HDFS_TACS}/month=${CONTENT_YEAR}-${CONTENT_MONTH}-01"
HDFS_PATH_BACKUP="${HDFS_TACS}/_month=${CONTENT_YEAR}-${CONTENT_MONTH}-01"

# Check if GZIP file have arrived
# -----------------------------------------------------------------------------
gzip_file=${gzip_file_path}/${FILE_NAME}.txt.gz
num_files=`ls ${gzip_file} 2> /dev/null | wc -l`
if [ "${num_files}" -eq "0" ]; then
    ERR_MSG="File not found."
    echo $ERR_MSG > ${LOGFILE_LOCAL_PATH}/.tmp_error
    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${LOGFILE_LOCAL_PATH}/.tmp_error ${HDFS_PATH}/_ERROR
    # Log message
    error "${ERR_MSG}: Check ${LOCAL_PATHS_PREFIX}/${LOCAL_INBOX} and see why there's no file called ${gzip_file_without_path}." >> ${LOG_FILE} 2>&1
    die 2 "${ERR_MSG}"
else
  # Check if the file was previously copied into HDFS
  # -----------------------------------------------------------------------------
  if hadoop fs -test -d ${HDFS_PATH}; then
    info "Renaming ${HDFS_PATH} to ${HDFS_PATH_BACKUP}"
    hadoop fs -mv ${HDFS_PATH} ${HDFS_PATH_BACKUP}

    exitCode=$?
    if [[ "${exitCode}" != "0" ]]; then
      die 3 "Backup failed with exit code ${exitCode}"
    fi
  fi
  
  hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${HDFS_PATH}
  
  gzip_file_without_path=$(basename $gzip_file)
  txt_file_without_path=${gzip_file_without_path/.gz/}
  txt_file=${gzip_file_path}/${txt_file_without_path}
  txt_file_zeropadded_without_path=zeropadded_${txt_file_without_path}
  txt_file_zeropadded=${gzip_file_path}/${txt_file_zeropadded_without_path}
  
  # Check file integrity for ${gzip_file_without_path}
  # -----------------------------------------------------------------------------
  info "Checking file integrity for ${gzip_file_without_path}..."
  gzip -t ${gzip_file} 2> /dev/null
  file_error=$?
  if [[ "${file_error}" != "0" ]]; then
    error "Incomplete or Corrupt file: ${gzip_file_without_path}" >> ${LOG_FILE} 2>&1
    die 4 "Incomplete or Corrupt file: ${gzip_file_without_path}"
  else
    # Copy file to backup inbox.
    # Descompress file
    # -----------------------------------------------------------------------------
    mkdir -p ${gzip_file_path}/${DEVICES_CATALOG}/${CONTENT_DATE}
    cp ${gzip_file} ${gzip_file_path}/${DEVICES_CATALOG}/${CONTENT_DATE}/.
    gzip -d ${gzip_file}
    exitCode=$?
    if [[ "${exitCode}" != "0" ]]; then
      die 5 "Failed to descompress ${gzip_file_without_path} with exit code ${exitCode}"
    fi
    
    # Add zeros to left in TAC column
    # -----------------------------------------------------------------------------
    # Log message
    info "Converting ${txt_file_without_path} file to ${txt_file_zeropadded_without_path}" >> ${LOG_FILE} 2>&1
    cat ${txt_file} | gawk -F'|' 'BEGIN { OFS="|"; } !/^TAC/{ $1=sprintf("%08d", $1); print $0 }' >> ${txt_file_zeropadded}
    exitCode=$?
    if [[ "${exitCode}" != "0" ]]; then
      die 6 "Failed to convert ${txt_file_without_path} with exit code ${exitCode}"
    fi
    
    # Put file converted to HDFS
    # -----------------------------------------------------------------------------
    # Log message
    info "Putting ${txt_file_without_path} file to HDFS" >> ${LOG_FILE} 2>&1
    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${txt_file_zeropadded} ${HDFS_PATH}/${txt_file_zeropadded_without_path}
    exitCode=$?
    if [[ "${exitCode}" != "0" ]]; then
      ERR_MSG="Can not put ${txt_file_without_path} file to HDFS"
      echo $ERR_MSG > ${LOGFILE_LOCAL_PATH}/.tmp_error
      hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${LOGFILE_LOCAL_PATH}/.tmp_error ${HDFS_PATH}/_ERROR
      die 7 "Failed to put ${txt_file_zeropadded} file to HDFS with exit code ${exitCode}"
    else
      hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -touchz ${HDFS_PATH}/_SUCCESS
      hadoop fs -rm -r ${HDFS_PATH_BACKUP}
    fi
    # Log message
    info "File ${txt_file_zeropadded_without_path} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01 SUCCESSFULLY UPLOADED" >> ${LOG_FILE} 2>&1

    # Move file converted to backup inbox.
    # Remove descompress file
    # -----------------------------------------------------------------------------
    # Log message
    info "Moving ${txt_file_zeropadded_without_path} to ${gzip_file_path}/${DEVICES_CATALOG}/${CONTENT_DATE}" >> ${LOG_FILE} 2>&1
    mv ${txt_file_zeropadded} ${gzip_file_path}/${DEVICES_CATALOG}/${CONTENT_DATE}/.
    info "Removing ${txt_file_without_path}" >> ${LOG_FILE} 2>&1
    rm ${txt_file}
  fi
fi
# Log message
info "${txt_file_zeropadded_without_path} processed" >> ${LOG_FILE} 2>&1

# Log message
info "Finished processing file. Log file will be available on HDFS: ${log_file_hdfs}" >> ${LOG_FILE} 2>&1

# BACKUP LOG TO HDFS
hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${LOGFILE_HDFS_PATH}
hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${LOG_FILE} ${LOGFILE_HDFS_PATH}

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
