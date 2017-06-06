#!/usr/bin/env bash
# 
# genfile
# -------
# 
# Generates the specified interface based on an existing template.
# 
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # /opt/gbic/services/gplatform/global/generation/genfile.sh [OPTION] {vers-num} {country} {file} [{yyyyMM} [{local_path_prefix}]]
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/generation/genfile.sh 5 ES DIM_M_BILLING_CYCLE 201501 {{ remote.genfileoutbox }}
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] MS_VERSION COUNTRY INTERFACE [MONTH] [LOCAL_PATH_PREFIX]"
  echo -e "Generates the specified interface based on an existing template\n"
  echo -e "\t-i, --ingest"
  echo -e "\t               execute ingestion script for specified file before generation."
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

DEFAULT_LOCAL_PATHS_PREFIX="{{ remote.genfileoutbox }}"

TEMPLATE_PATH=${GBIC_HOME}/generation/templates
TEMPLATE_PREFFIX=tpl
TEMPLATE_SUFFIX=.txt
FAKE_OB=XX

INGESTION_SCRIPT=${GBIC_HOME}/ingestion/ingest-files.sh

ALARM_URL=${GENFILE_ALARM_URL}

# Finalizes script with an error code and an alarm. It receives two arguments:
# - ERROR_CODE: numeric code of the error for ending the script
# - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
# Example:
#   die 27 "There is not connection to server"
die () {
  __die "${ALARM_URL}"    \
        ":bell:"          \
        "$1"              \
        "$2"              \
        "${SERVICE_NAME}" \
        "File generation script for ${OB_2M}_${INTERFACE}_${CONTENT_DATE}"
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

# ARGS: MS_VERSION, OBS_2M, MONTHLY_FILE, CONTENT_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
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
  cancel "wrong Interface Name of file to be generated: NOT RECEIVED"
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

PREVIOUS_MONTH=$(date "--date=${CONTENT_DATE}01 -1 month" +"%Y%m")
PREVIOUS_MONTH_YEAR=${PREVIOUS_MONTH:0:4}
PREVIOUS_MONTH_MONTH=${PREVIOUS_MONTH:4:2}

NEXT_MONTH=$(date "--date=${CONTENT_DATE}01 +1 month" +"%Y%m")
NEXT_MONTH_YEAR=${NEXT_MONTH:0:4}
NEXT_MONTH_MONTH=${NEXT_MONTH:4:2}

PREVIOUS_MONTH_LAST_DAY=$(date "--date=${PREVIOUS_MONTH}01 +1 month" +"%Y-%m-%d")
PREVIOUS_MONTH_LAST_DAY=$(date "--date=${PREVIOUS_MONTH_LAST_DAY} -1 day" +"%d")

CONTENT_LAST_DAY=$(date "--date=${CONTENT_DATE}01 +1 month" +"%Y-%m-%d")
CONTENT_LAST_DAY=$(date "--date=${CONTENT_LAST_DAY} -1 day" +"%d")

TEMPLATE_NAME=${TEMPLATE_PREFFIX}_${OB_2M}_${INTERFACE}${TEMPLATE_SUFFIX}
TEMPLATE_AUX_NAME=${TEMPLATE_PREFFIX}_${FAKE_OB}_${INTERFACE}${TEMPLATE_SUFFIX}
TEMPLATE=${TEMPLATE_PATH}/${TEMPLATE_NAME}
TEMPLATE_AUX=${TEMPLATE_PATH}/${TEMPLATE_AUX_NAME}

LOCAL_INBOX=${OB_3M}/GPLATFORM/MSv${MS_VERSION}
TARGET_FILE_PATH=${LOCAL_PATHS_PREFIX}/${LOCAL_INBOX}/${CONTENT_DATE}
TARGET_FILE_NAME=${OB_2M}_${INTERFACE}_01_${CONTENT_DATE}
TXT_FILE=${TARGET_FILE_PATH}/${TARGET_FILE_NAME}.txt
BZ2_FILE=${TARGET_FILE_PATH}/${TARGET_FILE_NAME}.bz2

# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
info "Launched Semantic Model Interface Generation Script on ${EXEC_TIME}"
info "OPTIONS:"
info "ARGUMENTS:"
info "|- Semantic Model Version of file to be generated: '${MS_VERSION}' (MSv${MS_VERSION})"
info "|- Country Code of file to be generated: '${OB_2M}'"
info "|- Interface of file to be generated: '${INTERFACE}'."
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
# Template reading
# -------------------------------------------------------------------------------------------------
info "Reading template..."
foundTemplate=
if [ -f ${TEMPLATE} ]; then
  info "Found ${OB_3M}'s specific template for ${INTERFACE}"
  foundTemplate=${TEMPLATE}
elif [ -f ${TEMPLATE_AUX} ]; then
  info "Found generic template for ${INTERFACE} (not country specific)"
  foundTemplate=${TEMPLATE_AUX}
fi
if [[ "${foundTemplate}" != "" ]]; then
  
  substScript=$(while read -r line; do
    echo "echo $line;"
  done < ${foundTemplate})
  
else
  die 2 "Template not found for ${INTERFACE} in ${TEMPLATE_PATH}"
fi

# -------------------------------------------------------------------------------------------------
# File writing preparation
# -------------------------------------------------------------------------------------------------
info "Ensuring existence of directory '${TARGET_FILE_PATH}'"
mkdir -p ${TARGET_FILE_PATH}
exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 3 "Target directory could not be created '${TARGET_FILE_PATH}'. Please, see log for details."
fi

# -------------------------------------------------------------------------------------------------
# Template syntax validation
# -------------------------------------------------------------------------------------------------
info "File ${TXT_FILE} will be generated with following content:"
echo;
eval $substScript
exitCode=$?
echo;
if [[ "${exitCode}" != "0" ]]; then
  die 4 "Syntax error on template file: ${foundTemplate}. Please, see log for details."
fi

# -------------------------------------------------------------------------------------------------
# TXT file writing
# -------------------------------------------------------------------------------------------------
info "Creating ${TARGET_FILE_NAME}.txt"
eval $substScript > ${TXT_FILE}
exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 5 "File could not be created. Please, see log for details."
fi

# -------------------------------------------------------------------------------------------------
# Compression of file
# -------------------------------------------------------------------------------------------------
info "Compressing file"
bzip2 -z ${TXT_FILE}
mv ${TXT_FILE}.bz2 ${BZ2_FILE}

exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 6 "BZ2 file could not be created. Please, see log for details."
fi

# -------------------------------------------------------------------------------------------------
# File ingestion
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_INGEST}" == "1" ]]; then
  info "Running file ingestion..."
  
  ${INGESTION_SCRIPT} ${MS_VERSION} ${OB_2M} ${INTERFACE} ${CONTENT_DATE} ${LOCAL_PATHS_PREFIX}
  
  exitCode=$?
  if [[ "${exitCode}" != "0" ]]; then
    die 7 "File ingestion failed with exit code ${exitCode}"
  else
    # workaround to the lack of return code of ingest-files script
    ingestionLog=`ls -rt ${LOCAL_PATHS_PREFIX}/${LOCAL_INBOX}/TEMP/log-ingest-files-*_*.log | tail -1`
    exitCode=`grep '#ERROR' ${ingestionLog} | wc -l`
    if [[ "${exitCode}" != "0" ]]; then
      die 2 "`grep '#ERROR' ${ingestionLog} | head -1 | gawk '{ $1=""; $2=""; $3=""; print $0 }' | sed 's/^[ ]*//g'`"
    fi
  fi
  info "Finished file ingestion"
else
  info "No file ingestion required"
fi

# -------------------------------------------------------------------------------------------------
info "Finished process. SUCCESS!"
