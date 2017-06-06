#!/usr/bin/env bash
# 
# prereqs.sh
# -------
# 
# Executes pre-requirements needed before executing ETL processes. The steps executed are:
#     1) Generation and ingestion of files needed by the OB
# 
# Usage: ** Execute script with --help for details on usage.
# 
# # /opt/gbic/services/gplatform/global/etl/prereqs/prereqs.sh [OPTION] {vers-num} {country} {file-list} [{yyyyMM} {local_path_prefix}]
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/etl/prereqs/prereqs.sh 5 ES "DIM_M_GROUP_SVA DIM_M_BILLING_CYCLE" 201601 /sftp
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] MS_VERSION COUNTRY LIST_OF_FILES [MONTH] [LOCAL_PATH_PREFIX]"
  echo -e "Executes pre-requirements for COUNTRY needed by ETL processes\n"
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized MONTH format:"
  echo -e "\tYYYYMM"
}

GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/../.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-common.sh


# OPTIONS: -i
###################################################################################################
OPT_INGEST=""

while getopts '\-:i' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        ingest         ) OPT_INGEST=-i;;
        ingest*        ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        help           ) help;;
        # "--" terminates argument processing
        ''             ) break;;
        *              ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
     i) OPT_INGEST=-i;;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# ARGS: MS_VERSION, CONTENT_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
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
# Validate LIST_OF_FILES
# -------------------------------------------------------------------------------------------------
LIST_OF_FILES="$3"
if [[ "${LIST_OF_FILES}" == "" ]]; then
  cancel "wrong list of files to be generated: NOT RECEIVED"
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
    LOCAL_PATH_PREFIX="{{ remote.genfileoutbox }}"
else
    LOCAL_PATH_PREFIX=${PREFIX}
fi


# MAIN PROCESS
###################################################################################################
# -------------------------------------------------------------------------------------------------
# Files generation
# -------------------------------------------------------------------------------------------------
info "Generating files..."

for file in ${LIST_OF_FILES}; do
    ${GBIC_HOME}/generation/genfile.sh ${OPT_INGEST} ${MS_VERSION} ${OB_2M} ${file} ${CONTENT_DATE} ${LOCAL_PATH_PREFIX}
    rc=$?;
    if [[ $rc != 0 ]]; then
        exit $rc;
    fi
done

# -------------------------------------------------------------------------------------------------
info "Finished process. SUCCESS!"
