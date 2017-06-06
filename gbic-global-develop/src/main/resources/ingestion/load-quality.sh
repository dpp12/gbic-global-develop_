#!/usr/bin/env bash
# 
# Load quality files process
# --------------------------
# 
# Load quality files to mysql table.
# It has to be executed as admin user.
# It has to have read access to mysql gbic_data_quality database
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # ${GPLATFORM_HOME}/ingestion/load-quality.sh {version} {country} {dataset_quality_list} [{yyyyMM} [{local_path_prefix}]]
# 
# Example:
# 
# # ${GPLATFORM_HOME}/ingestion/load-quality.sh MSv5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201601
#
# Return codes:
#  0: Success
#  1: INVOCATION ERROR. Wrong argument
#  2: OPERATION ERROR. File(s) not found.
#
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] COUNTRY DATASET_QUALITY_LIST [ MONTH ]"
  echo -e "Load data quality files to mysql\n"
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
        "Data Quality MySQL Loading script for '${OB_2M}', '${DATASET}' and ${CONTENT_DATE}"
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


# ARGS: VERSION, OB_2M, DATASET_QUALITY_LIST, CONTENT_DATE (optional) Format yyyyMM
###################################################################################################
# Validate VERSION
# -------------------------------------------------------------------------------------------------
VERSION=$1
if [[ "${VERSION}" == "" ]]; then
  cancel "wrong Version of file to be generated: NOT RECEIVED"
fi
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
DATASETS=$3
if [[ "${DATASETS}" == "" ]]; then
  cancel "wrong Dataset list: NOT RECEIVED"
else
  for dataset in ${DATASETS}; do
    validateDataset ${dataset}; validDataset=$?
    if [[ "$validDataset" = "0" ]]; then
      cancel "wrong quality file name: '${dataset}' NOT AVAILABLE. Please, choose one of the following: { ${DEFAULT_DATASETS//[ ]/, } }."
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

OB_3m=$(getOB_3m ${OB_2M})
OB_3M=$(getOB_3M ${OB_2M})
OP_ID=$(getOpId  ${OB_2M})

QUALITY=QUALITY

LOCAL_INBOX="${DEFAULT_LOCAL_PATH_PREFIX}/${OB_3M}/GPLATFORM/${VERSION}/${CONTENT_DATE}"

# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched load DQ files script on ${EXEC_TIME}"
debug "OPTIONS:"
debug "ARGUMENTS:"
debug "|- Country Code: '${OB_2M}'"
debug "|- Quality file to be checked: '${DATASETS}'."
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date to be checked not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date to be checked: '${CONTENT_DATE}'"
fi

# -------------------------------------------------------------------------------------------------
# Processing OB.
# -------------------------------------------------------------------------------------------------
for DATASET in ${DATASETS}; do
    
    info "Processing Quality file: ${DATASET}" 
    
    file_bz=${LOCAL_INBOX}/${OB_2M}_${DATASET}_*_${CONTENT_DATE}_${QUALITY}.bz2
    bz_file_list=`ls ${file_bz} 2> /dev/null`
    num_files=`ls ${bz_file_list} 2> /dev/null | wc -l`
    
    # Check if BZ2 quality file(s) have arrived
    if [ "${num_files}" -eq "0" ]; then
      die 2 "OPERATION ERROR. File(s) not found."
    fi
    ERRORS=0
    
    for bz2_file in ${bz_file_list}; do
        bz2_file_without_path=$(basename $bz2_file)
        txt_file_without_path=${bz2_file_without_path/.bz2/.txt}
        txt_file=${LOCAL_INBOX}/${txt_file_without_path}
        
        bzcat ${bz2_file} > ${txt_file}
        mysql -h {{ db.host }} -u {{ db.user }} -p{{ db.pass }} -P{{ db.port }} --local_infile=1\
          -e"LOAD DATA LOCAL INFILE '${txt_file}' INTO TABLE {{ db.schema_dq }}.service_checks FIELDS TERMINATED BY '|';"
        rm ${txt_file}
    done
done

info "Finished processing quality files of ${OB_3M}."

# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
