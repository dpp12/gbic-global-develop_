#!/usr/bin/env bash
# 
# GPlatform workflow
# ------------------
# 
# Executes the whole ingestion workflow for a given OB and month, followed by the call of the ETL oozie workflow.
#
# It has to be executed as admin user.
# It has to have write access to local inbox home directory, p.e: { remote.inbox }/{OB}/GPLATFORM/*
# and then launches the ETL oozie workflow.
#
# Usage: (as admin) ** Execute script with --help for details on usage.
# 
# # { project_home }/workflow/gplatform.sh {vers-num} {country} {dataset-list} [{yyyyMM} [{local_path_prefix}]]
# 
# Example:
# 
# # { project_home }/workflow/gplatform.sh 5 ES "CUSTOMER M_LINES DIM_M_TARIFF_PLAN" 201606
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
showHelp () {
  echo -e "Usage: $0 [OPTION] VERSION COUNTRY DATASET_LIST [ MONTH [LOCAL_PATH_PREFIX] ]"
  echo -e "Executes the whole ingestion workflow for a given OB and month, followed by the call of the ETL oozie workflow.\n"
  echo -e "\t-Q, --ignore-dq-files"
  echo -e "\t               data quality files will be ignored."
  echo -e "\t               It won't be enabled by default (so ingest and load scripts for DQ will run)."
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
  echo -e "\t-f FROM_STEP, --from=FROM_STEP"
  echo -e "\t               execute workflow from this step (included)."
  echo -e "\t               If not specified, FIRST step will be used."
  echo -e "\t-t TO_STEP, --to=TO_STEP"
  echo -e "\t               execute workflow to this step (included)."
  echo -e "\t               If not specified, LAST step will be used."
  echo -e "\t    --help     display this help and exit"
  echo -e "\nRecognized FROM_STEP and TO_STEP values:"
  echo -e "\tThis pipeline is a Directed Acyclic Graph wich is executed in predefined steps."
  echo -e "\tFROM_STEP and TO_STEP should be one of the following:"
  echo -e "\t* 'pre': Pre-Ingestion of files: It empties the spool directory to correct inbox"
  echo -e "\t* 'ing': Ingestion of files and load quality files:"
  echo -e "\t         It ingests files and quality files to HDFS directory and load quality files to mysql."
  echo -e "\t         If --ignore-dq-files is specified, only data files will be ingested."
  echo -e "\t         There are several ingestion scripts, executed according to OB/file specific riules:"
  echo -e "\t         - ingest-files: puts data files into HDFS inbox"
  echo -e "\t         - ingest-TRAFFIC-files: puts TRAFFIC data files into HDFS inbox (using HIVE)"
  echo -e "\t         - ingest-quality-files: puts QUALITY files info HDFS inbox"
  echo -e "\t         - load-quality-files: loads QUALITY screens into MySQL DQ data model"
  echo -e "\t* 'owf': Runs and monitorizes Oozie workflow"
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

PRE_INGESTION_SCRIPT=${GBIC_HOME}/ingestion/pre-ingest.sh
INGESTION_QUALITY_SCRIPT=${GBIC_HOME}/ingestion/ingest-quality.sh
INGESTION_SCRIPT=${GBIC_HOME}/ingestion/ingest-files.sh
INGESTION_TRAFFICS_SCRIPT=${GBIC_HOME}/ingestion/ingest-TRAFFIC-files.sh
LOAD_QUALITY_SCRIPT=${GBIC_HOME}/ingestion/load-quality.sh
RUN_OOZIE_SCRIPT=${GBIC_HOME}/workflow/run-oozie-workflow.sh
MONITOR_OOZIE_SCRIPT=${GBIC_HOME}/workflow/monitor-oozie-workflow.sh

ALARM_URL=${WORKFLOW_ALARM_URL}

# Finalizes script with an error code and an alarm. It receives two arguments:
# - ERROR_CODE: numeric code of the error for ending the script
# - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
# Example:
#   die 27 "There is not connection to server"
die () {
  __die "${ALARM_URL}" \
        ":bug:"           \
        "$1"              \
        "$2"              \
        "${SERVICE_NAME}" \
        "Gplatform pipeline for '${OB_2M}', '${DATASETS}' and ${CONTENT_DATE}"
}


# OPTIONS: 
###################################################################################################
IGNORE_DQ_FILES=0
SKIP_DQ=0
SKIP_LOCAL=0
SKIP_GLOBAL=0
AUTO_PROMOTE=0
FROM=
TO=

while getopts '\-:Qqlgp-f:-t:' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        ignore-dq-files  ) IGNORE_DQ_FILES=1;;
        ignore-dq-files* ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        skip-dq          ) SKIP_DQ=1;;
        skip-dq*         ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        skip-local       ) SKIP_LOCAL=1;;
        skip-local*      ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        skip-global      ) SKIP_GLOBAL=1;;
        skip-global*     ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        auto-promote     ) AUTO_PROMOTE=1;;
        auto-promote*    ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        from=?*          ) FROM=${long_optarg};;
        from*            ) cancel "option requires an argument -- ${OPTARG}";;
        to=?*            ) TO=${long_optarg};;
        to*              ) cancel "option requires an argument -- ${OPTARG}";;
        help             ) help;;
        # "--" terminates argument processing
        ''               ) break;;
        *                ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
    Q ) IGNORE_DQ_FILES=1;;
    q ) SKIP_DQ=1;;
    l ) SKIP_LOCAL=1;;
    g ) SKIP_GLOBAL=1;;
    p ) AUTO_PROMOTE=1;;
    f ) FROM=${OPTARG};;
    t ) TO=${OPTARG};;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# -------------------------------------------------------------------------------------------------
# Validate FROM
# -------------------------------------------------------------------------------------------------
FROM_STEP=
if [[ "${FROM}" == "" ]]; then
  FROM_STEP=${MIN_GPLATFORM_PIPELINE_STEPS}
else
  validateWFStep ${FROM}; validStep=$?
  if [[ "$validStep" = "0" ]]; then
    cancel "wrong initial step: '${FROM}' NOT AVAILABLE. Please, choose one of the following: { ${GPLATFORM_PIPELINE_STEPS//[ ]/, } }."
  fi
  FROM_STEP=${validStep}
fi
# -------------------------------------------------------------------------------------------------
# Validate TO
# -------------------------------------------------------------------------------------------------
TO_STEP=
if [[ "${TO}" == "" ]]; then
  TO_STEP=${MAX_GPLATFORM_PIPELINE_STEPS}
else
  validateWFStep ${TO}; validStep=$?
   if [[ "$validStep" = "0" ]]; then
    cancel "wrong final step: '${TO}' NOT AVAILABLE. Please, choose one of the following: { ${GPLATFORM_PIPELINE_STEPS//[ ]/, } }."
  elif [[ "${validStep}" < "${FROM_STEP}" ]]; then
    cancel "wrong final step: '${TO}' happens before '${FROM}'. This is DAG's order: ${GPLATFORM_PIPELINE_STEPS//[ ]/ > }"
  fi
  TO_STEP=${validStep}
fi


# ARGS: VERSION, OB_2M, DATASETS, CONTENT_DATE (optional) Format yyyyMM, LOCAL_PATH_PREFIX (optional)
###################################################################################################
# Validate VERSION
# -------------------------------------------------------------------------------------------------
VERSION=$1
if [[ "${VERSION}" == "" ]]; then
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
# -------------------------------------------------------------------------------------------------
# Validate LOCAL_PATH_PREFIX
# -------------------------------------------------------------------------------------------------
PREFIX=$5
if [[ "${PREFIX}" == "" ]]; then
  LOCAL_PATH_PREFIX=${DEFAULT_LOCAL_PATH_PREFIX}
else
  LOCAL_PATH_PREFIX=${PREFIX}
fi

# -------------------------------------------------------------------------------------------------
# process options to initialize flags
# -------------------------------------------------------------------------------------------------
FLAG_SKIP_DQ=
FLAG_SKIP_LOCAL=
FLAG_SKIP_GLOBAL=
FLAG_AUTO_PROMOTE=

if [[ "${SKIP_DQ}"      == "1" ]]; then FLAG_SKIP_DQ="--skip-dq"; fi
if [[ "${SKIP_LOCAL}"   == "1" ]]; then FLAG_SKIP_LOCAL="--skip-local"; fi
if [[ "${SKIP_GLOBAL}"  == "1" ]]; then FLAG_SKIP_GLOBAL="--skip-global"; fi
if [[ "${AUTO_PROMOTE}" == "1" ]]; then FLAG_AUTO_PROMOTE="--auto-promote"; fi


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`
TIMESTAMP=${EXEC_TIME//[ :-]/}

CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}

LOCAL_INBOX_PER_OB="GPLATFORM/MSv${VERSION}"
TEMP_LOCAL_PATH_PER_OB="TEMP/${CONTENT_DATE}"

OB_3m=$(getOB_3m ${OB_2M})
OB_3M=$(getOB_3M ${OB_2M})
OP_ID=$(getOpId  ${OB_2M})

LOCAL_INBOX=${LOCAL_PATH_PREFIX}/${OB_3M}/${LOCAL_INBOX_PER_OB}
TEMP_LOCAL_PATH=${LOCAL_INBOX}/${TEMP_LOCAL_PATH_PER_OB}

JOBID_FILE_PREFIX="JOBID"
JOBSTATUS_FILE_PREFIX="JOBSTATUS"


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity.
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
# Console messages
debug "Launched Gplatorm pipeline script for MSv${VERSION} on ${EXEC_TIME}"
debug "OPTIONS:"
debug "|- Data Quality:"
debug "|- Launched Gplatorm pipeline script from '${FROM}' (${FROM_STEP}) to '${TO}' (${TO_STEP}), both included"
if [[ "${IGNORE_DQ_FILES}" == "0" ]]; then
  debug "|- ignore-dq-files flag not setted. Data quality files will be loaded in HDFS and MySQL (default)"
else
  debug "|- ignore-dq-files flag setted. Data quality files will be ignored"
fi
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
debug "ARGUMENTS:"
debug "|- Semantic Model Version of files to be ingested: '${VERSION}' (MSv${VERSION})"
debug "|- Country Code of files to be ingested: '${OB_2M}'"
debug "|- Datasets to be processed: '${DATASETS}'."
if [[ "${MONTH}" == "" ]]; then
  # Console message
  debug "|- Date of file to be processed not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  # Console message
  debug "|- Date of file to be processed: '${CONTENT_DATE}'"
fi
if [[ "${PREFIX}" == "" ]]; then
  # Console message
  debug "|- Root folder for file to be ingested not specified. Default will be used: '${LOCAL_PATH_PREFIX}'"
else
  # Console message
  debug "|- Root folder for file to be ingested: '${LOCAL_PATH_PREFIX}'"
fi

# -------------------------------------------------------------------------------------------------
# Create paths 
# -------------------------------------------------------------------------------------------------
mkdir -p ${TEMP_LOCAL_PATH}

# -------------------------------------------------------------------------------------------------
# Process will be::
# | 1. If OB is Spain: run pre-ingest
# | 2. Ingest stage for OBs:
#   | 2.1 If Dataset is TRAFFIC_VOICE or DAILY_TRAFFICES and OB is Spain:
#         Execute ingest-TRAFFIC_VOICE-files.sh or ingest-DAILY_TRAFFICES-files.sh
#   | 2.2 If Dataset is TRAFFIC_VOICE, TRAFFIC_SMS or TRAFFIC_DATA and OB is Brasil:
#         Execute ingest-TRAFFIC_VOICE-files.sh ingest-TRAFFIC_SMS-files.sh or ingest-TRAFFIC_DATA-files.sh
#   | 2.3 Other cases:
#     | 2.3.1 Check if file exists on HDFS.
#             If exists--> Backup
#     | 2.3.2 Execute ingest-files and ingest-quality process
#   | 2.4 If Dataset is NOT TRAFFIC_VOICE or DAILY_TRAFFICES and OB is Spain:
#     | 2.4.1 Check if file exists on HDFS.
#             If exists--> Backup
#     | 2.4.2 Execute ingest-quality.sh
#     | 2.4.3 Load quality files to MYSQL.
# | 3. Run script to launch the coordinator.
# | 4. Monitor Oozie Workflow
# -------------------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------------------
# 1. If OB is Spain: run pre-ingest
# -------------------------------------------------------------------------------------------------
validateWFStep pre; preIngestStep=$?
if [[ "${preIngestStep}" -ge "${FROM_STEP}" &&  "${preIngestStep}" -le "${TO_STEP}" ]]; then
  if [[ "${OB_2M}" == "ES" || "${OB_2M}" == "BR" || "${OB_2M}" == "CL" || "${OB_2M}" == "CO" || "${OB_2M}" == "MX" ]]; then
    
    info "--------------------------------------"
    info "STAGE: pre-ingest.sh for ${OB_2M}     "
    info "--------------------------------------"
    
    info "Executing pre-ingest for ${OB_2M}. Launched at $EXEC_TIME"
    
    ${PRE_INGESTION_SCRIPT} MSv${VERSION} "${LOCAL_PATH_PREFIX}/${OB_3M}/GPLATFORM" ${OB_2M}
    
    exitCode=$?
    if [[ "${exitCode}" != "0" ]]; then
      die 2 "File pre-ingest failed with exit code ${exitCode}"
    fi
  fi
fi

# -------------------------------------------------------------------------------------------------
# 2. Ingest stage for OBs
# -------------------------------------------------------------------------------------------------
validateWFStep ing; ingestStep=$?
if [[ "${ingestStep}" -ge "${FROM_STEP}" &&  "${ingestStep}" -le "${TO_STEP}" ]]; then
  for INTERFACE in ${DATASETS}; do
    
    info "|-- Ingesting ${INTERFACE}"
    
    # --------------------------------------------------------------------------------------------------------------------
    # 2.1 If Dataset is TRAFFIC_VOICE or DAILY_TRAFFICES and OB is Spain:
    #     Execute ingest-TRAFFIC_VOICE-files.sh or ingest-DAILY_TRAFFICES-files.sh
    # 2.2 If Dataset is TRAFFIC_VOICE, TRAFFIC_SMS or TRAFFIC_DATA and OB is Brasil:
    #     Execute ingest-TRAFFIC_VOICE-files.sh ingest-TRAFFIC_SMS-files.sh or ingest-TRAFFIC_DATA-files.sh
    # --------------------------------------------------------------------------------------------------------------------
    if [[ "${OB_2M}" == "ES" && ("${INTERFACE}" == "TRAFFIC_VOICE"  ||
                                 "${INTERFACE}" == "DAILY_TRAFFIC") ||
          "${OB_2M}" == "BR" && ("${INTERFACE}" == "TRAFFIC_DATA"   ||
                                 "${INTERFACE}" == "TRAFFIC_SMS"    ||
                                 "${INTERFACE}" == "TRAFFIC_VOICE") ]]
    then
      
      info "--------------------------------------"
      info "STAGE: ingest-TRAFFIC-file.sh         "
      info "--------------------------------------"
      info "Processing Traffics of ${OB_3M}: ${INTERFACE}"
      
      ${INGESTION_TRAFFICS_SCRIPT} -i ${VERSION} ${OB_2M} ${INTERFACE} ${CONTENT_DATE} ${LOCAL_PATH_PREFIX}
      
      exitCode=$?
      if [[ "${exitCode}" != "0" ]]; then
        die 3 "File ingest-traffics for ${OB_2M} failed with exit code ${exitCode}"
      fi
      
    # --------------------------
    # 2.3 Other cases
    # --------------------------
    else
      
      info "Processing Datasets of Countries: ${INTERFACE} for ${OB_2M}"
      
      info "--------------------------------------"
      info "STAGE: ingest-files.sh                "
      info "--------------------------------------"
      
      HDFS_PATH_OLD="${HDFS_INBOX}/${OB_3m}/MSv${VERSION}/${INTERFACE}/month=${CONTENT_YEAR}-${CONTENT_MONTH}-01"
      HDFS_PATH_NEW="${HDFS_INBOX}/${OB_3m}/MSv${VERSION}/${INTERFACE}/_month=${CONTENT_YEAR}-${CONTENT_MONTH}-01_`date '+%Y%m%d%H%M%S'`"
      
      # 2.3.1 Check if file exists on HDFS
      #       If exists--> Backup
      if hadoop fs -test -d ${HDFS_PATH_OLD}; then
        
        info "Renaming ${HDFS_PATH_OLD} to ${HDFS_PATH_NEW}"
        
        hadoop fs -mv ${HDFS_PATH_OLD} ${HDFS_PATH_NEW}
        
        exitCode=$?
        if [[ "${exitCode}" != "0" ]]; then
          die 4 "Backup failed with exit code ${exitCode}"
        fi
      fi
      
      # 2.3.2 Execute ingest-files
      info "Processing ingest-files of Countries: ${INTERFACE} for ${OB_2M}"
      
      ${INGESTION_SCRIPT} ${VERSION} ${OB_2M} ${INTERFACE} ${CONTENT_DATE} ${LOCAL_PATH_PREFIX}
      
      exitCode=$?
      if [[ "${exitCode}" != "0" ]]; then
        die 5 "File ingest-files failed with exit code ${exitCode}"
      else
        # workaround to the lack of return code of ingest-files script
        ingestionLog=`ls -rt ${LOCAL_INBOX}/TEMP/log-ingest-files-*_*.log | tail -1`
        exitCode=`grep '#ERROR' ${ingestionLog} | wc -l`
        if [[ "${exitCode}" != "0" ]]; then
          die 5 "`grep '#ERROR' ${ingestionLog} | head -1 | gawk '{ $1=""; $2=""; $3=""; print $0 }' | sed 's/^[ ]*//g'`"
        fi
      fi
    fi
    
    # --------------------------------------------------------------------------------------------------------------------
    # 2.4 If Dataset is NOT TRAFFIC_VOICE or DAILY_TRAFFICES and OB is Spain:
    #     Execute ingest-quality.sh
    # --------------------------------------------------------------------------------------------------------------------
    if [[ "${IGNORE_DQ_FILES}" == "1" ]]; then
      # WARNING! Data QUALITY files WILL NOT be ingested in HDFS nor loaded into MySQL
      warn "Data QUALITY files WILL NOT be ingested in HDFS nor loaded into MySQL"
      
    elif [[ "${OB_2M}" != "ES" || ("${INTERFACE}" != "TRAFFIC_VOICE"  &&
                                   "${INTERFACE}" != "DAILY_TRAFFIC") ]]
    then
      
      info "Processing QUALITY files for:  ${OB_2M}'s ${INTERFACE} "
      
      info "--------------------------------------"
      info "STAGE: ingest-quality.sh              "
      info "--------------------------------------"
      
      DATASET_LOWERCASE=$(echo "${INTERFACE}" | awk '{print tolower($0)}')
      
      HDFS_PATH_OLD="${HDFS_SRVCHECKS}/${OB_3m}/${DATASET_LOWERCASE}/day=${CONTENT_YEAR}-${CONTENT_MONTH}-01"
      HDFS_PATH_NEW="${HDFS_SRVCHECKS}/${OB_3m}/${DATASET_LOWERCASE}/_day=${CONTENT_YEAR}-${CONTENT_MONTH}-01_`date '+%Y%m%d%H%M%S'`"
      
      # 2.4.1 Check if file exists on HDFS
      #       If exists--> Backup
      if hadoop fs -test -d ${HDFS_PATH_OLD}; then
        
        info "Renaming ${HDFS_PATH_OLD} to ${HDFS_PATH_NEW}"
        
        hadoop fs -mv ${HDFS_PATH_OLD} ${HDFS_PATH_NEW}
        
        exitCode=$?
        if [[ "${exitCode}" != "0" ]]; then
          die 6 "Backup failed with exit code ${exitCode}"
        fi
      fi
      
      # 2.4.2 Execute ingest-quality.sh
      info "Processing ingest-quality of Countries: ${INTERFACE} for ${OB_2M}"
      
      ${INGESTION_QUALITY_SCRIPT} MSv${VERSION} ${OB_2M} ${INTERFACE} ${CONTENT_DATE} ${LOCAL_PATH_PREFIX}
      
      exitCode=$?
      if [[ "${exitCode}" != "0" ]]; then
        die 7 "File ingest-quality failed with exit code ${exitCode}"
      else
        # workaround to the lack of return code of ingest-quality script
        ingestionLog=`ls -rt ${LOCAL_INBOX}/TEMP/log-ingest-files-*_*.log | tail -1`
        exitCode=`grep '#ERROR' ${ingestionLog} | wc -l`
        if [[ "${exitCode}" != "0" ]]; then
          die 7 "`grep '#ERROR' ${ingestionLog} | head -1 | gawk '{ $1=""; $2=""; $3=""; print $0 }' | sed 's/^[ ]*//g'`"
        fi
      fi
      
      # 2.4.2. Load quality files to MYSQL.
      info "--------------------------------------"
      info "STAGE: load-quality.sh                "
      info "--------------------------------------"
      
      info "Loading quality files to MYSQL for ${OB_2M}: ${DATASETS}"
      
      ${LOAD_QUALITY_SCRIPT} MSv${VERSION} ${OB_2M} ${INTERFACE} ${CONTENT_DATE}
      
      exitCode=$?
      if [[ "${exitCode}" != "0" ]]; then
        die 8 "Load quality files to mysql failed with exit code ${exitCode}"
      fi
    fi
  done
fi

# 3. Run script to launch the coordinator.
validateWFStep owf; oozieWorkflowStep=$?
if [[ "${oozieWorkflowStep}" -ge "${FROM_STEP}" &&  "${oozieWorkflowStep}" -le "${TO_STEP}" ]]; then
  info "--------------------------------------"
  info "STAGE: run-oozie-workflow.sh          "
  info "--------------------------------------"
  
  info "Processing run oozie workflow for ${OB_2M}: ${DATASETS}"
  
  JOBID_FILE=${TEMP_LOCAL_PATH}/${JOBID_FILE_PREFIX}_${TIMESTAMP}.txt
  ${RUN_OOZIE_SCRIPT} -f ${JOBID_FILE} ${FLAG_SKIP_DQ} ${FLAG_SKIP_LOCAL} ${FLAG_SKIP_GLOBAL} ${FLAG_AUTO_PROMOTE} ${VERSION} ${OB_2M} "${DATASETS}" ${CONTENT_DATE}
  
  exitCode=$?
  if [[ "${exitCode}" != "0" ]]; then
    die 9 "Run Ooozie Workflow failed with exit code ${exitCode}"
  fi

  # 4. Monitor Oozie Workflow
  info "--------------------------------------"
  info "STAGE: monitor-oozie-workflow.sh      "
  info "--------------------------------------"
  
  if [ ! -f ${JOBID_FILE} ]; then
    die 10 "No jobid file was found in ${TEMP_LOCAL_PATH}. Cannot monitor oozie workflow"
  fi
  JOB_ID=`cat ${JOBID_FILE}`
  
  JOBSTATUS_FILE=${TEMP_LOCAL_PATH}/${JOBSTATUS_FILE_PREFIX}_${TIMESTAMP}.txt
  
  ${MONITOR_OOZIE_SCRIPT} -f ${JOBSTATUS_FILE} ${JOB_ID}
  exitCode=$?
  status=`cat ${JOBSTATUS_FILE}`
  
  info "Removing jobid and jobstatus file..."
  rm ${JOBID_FILE}
  rm ${JOBSTATUS_FILE}
  
  if [[ "${exitCode}" == "0" ]]; then
    info "Oozie job finished with status SUCCEEDED"
  else
    die 11 "Oozie job for ${OB_2M} in ${MONTH} and datasets ${DATASETS} finished with status: ${status}"
  fi
fi


# -------------------------------------------------------------------------------------------------
# Console message
debug "Finished process. SUCCESS!"
