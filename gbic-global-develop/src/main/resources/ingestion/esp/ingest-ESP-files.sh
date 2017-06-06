#!/usr/bin/env bash
#
# gplatform ingestion process (ESP's "LTV" files)
# -----------------------------------------------
#
# Takes files from ${LOCAL_INBOX} and distributes them by name into HDFS
# It has to be executed as 'hdfs' user.
# 
# Usage: (as javierb)
# 
# # /opt/gbic/services/gplatform/global/ingestion/ingest-ESP-files.sh [{yyyyMM} [{local_path_prefix}]]
# 
###################################################################################################


# ARGS: CONTENT_DATE (optional) - Format yyyyMM, LOCAL_PATH_PREFIX (optional)
###################################################################################################
execTime=`date '+%F %H:%M:%S'`

if [[ "$1" == "" ]]; then
  LOAD_DATE=`date '+%Y%m'`
  CONTENT_DATE=$(date "--date=${LOAD_DATE}01 -1 month" +"%Y%m")
  echo "`date '+%F %H:%M:%S'` #INFO  Argument not received. Default date will be used (previous month '${CONTENT_DATE}')"
else
  CONTENT_DATE=$1
  echo "`date '+%F %H:%M:%S'` #INFO  Argument '${CONTENT_DATE}' received. "
fi

if [[ "$2" == "" ]]; then
    LOCAL_PATHS_PREFIX="{{ remote.inbox }}"
else
    LOCAL_PATHS_PREFIX=$2
fi


# CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
LOCAL_INBOX="LTV"
HDFS_UMASK={{ hdfs.umask }}
HDFS_INBOX="/user/gplatform/inbox/esp/legacy"

TEMP_LOCAL_PATH="${LOCAL_INBOX}/tmp"
LOGFILE_NAME=log-ingest-files-${execTime//[ ]/_}.log
LOGFILE_NAME=${LOGFILE_NAME//[:]/-}
LOGFILE_LOCAL_PATH="${TEMP_LOCAL_PATH}/logs"
LOGFILE_HDFS_PATH=${HDFS_INBOX}/_ingestion-logs
LOG_FILE_HDFS=${LOGFILE_HDFS_PATH}/${LOGFILE_NAME}

declare -A COLUMNS=(
             ["ANTIG_TERM"]='$2"|"$3"|"$4"|"$5'
                 ["CANJES"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14"|"$15'
        ["CONT_COMPROMISO"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10'
              ["CONTRATOS"]='$2"|"$3"|"$4"|"$5'
         ["ESTADOS_LINEAS"]='$2"|"$3"|"$4'
          ["INF_LIN_TERM1"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14"|"$15"|"$16"|"$17"|"$18"|"$19"|"$20"|"$21"|"$22"|"$23"|"$24"|"$25"|"$26"|"$27"|"$28"|"$29"|"$30"|"$31"|"$32"|"$33"|"$34"|"$35"|"$36"|"$37"|"$38"|"$39"|"$40"|"$41"|"$42"|"$43"|"$44"|"$45"|"$46"|"$47"|"$48"|"$49"|"$50"|"$51"|"$52"|"$53"|"$54"|"$55"|"$56"|"$57"|"$58"|"$59"|"$60"|"$61"|"$62"|"$63'
     ["INF_LINEA_VALOR_EM"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14"|"$15"|"$16"|"$17"|"$18"|"$19"|"$20"|"$21"|"$22"|"$23"|"$24"|"$25"|"$26"|"$27"|"$28"|"$29"|"$30"|"$31"|"$32"|"$33"|"$34"|"$35"|"$36"|"$37"|"$38"|"$39"|"$40"|"$41"|"$42'
        ["INF_LINEA_VALOR"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14"|"$15"|"$16"|"$17"|"$18"|"$19"|"$20"|"$21"|"$22"|"$23"|"$24"|"$25"|"$26"|"$27"|"$28"|"$29"|"$30"|"$31"|"$32"|"$33"|"$34"|"$35"|"$36'
        ["LINEAS_MULTISIM"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8'
       ["LINEAS_SERVICIOS"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7'
          ["MOV_DESC_TERM"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8'
               ["MOV_TERM"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8'
  ["MOVIMIENTOS_SERVICIOS"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9'
            ["POBLACIONES"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10'
  ["SEGMENTO_ORGANIZATIVO"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13'
         ["SEGMENTOS_TERM"]='$2"|"$3"|"$4"|"$5"|"$6'
         ["TERM_VOZDATOS1"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13"|"$14'
         ["TERM_VOZDATOS2"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9"|"$10"|"$11"|"$12"|"$13'
           ["TRAF_VOZ_HRC"]='$0'
            ["A_IMEISHIST"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9'
            ["B_IMEISHIST"]='$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8"|"$9'
)

declare -A OBS=(
    ["ES"]="esp"
)
DAY="01"


# INITIALIZATION
###################################################################################################
CONTENT_YEAR=${CONTENT_DATE:0:4}
CONTENT_MONTH=${CONTENT_DATE:4:2}


# PROCESS FILES
###################################################################################################
for ob2M in "${!OBS[@]}"; do
    
    ob3m=${OBS[$ob2M]}
    ob3M=${ob3m^^}
    
    LOG_FILE=${LOCAL_PATHS_PREFIX}/${ob3M}/${LOGFILE_LOCAL_PATH}/${LOGFILE_NAME}
    TEMP_LOCAL_PATH=${LOCAL_PATHS_PREFIX}/${ob3M}/${TEMP_LOCAL_PATH}
    RAR_FILE_PATH=${LOCAL_PATHS_PREFIX}/${ob3M}/${LOCAL_INBOX}/
    
    echo "`date '+%F %H:%M:%S'` #INFO  Searching for files of ${ob3M} in ${RAR_FILE_PATH}. Launched at $execTime" > ${LOG_FILE} 2>&1
    
    for file in "${!COLUMNS[@]}"; do
        
        echo "`date '+%F %H:%M:%S'` #INFO  Processing ${file}" >> ${LOG_FILE} 2>&1
        
        HDFS_PATH="${HDFS_INBOX}/${file}/month=${CONTENT_YEAR}-${CONTENT_MONTH}-01"
        hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${HDFS_PATH}
        if hadoop fs -test -e ${HDFS_PATH}/_ERROR ; then
            hadoop fs -rm -skipTrash ${HDFS_PATH}/_ERROR > /dev/null 2>&1
        fi
        
        # Check if the file was previously copied into HDFS
        RAR_FILE_GLOB=${RAR_FILE_PATH}/${CONTENT_DATE}_${file}.rar
        NUM_FILES=`ls ${RAR_FILE_GLOB} 2> /dev/null | wc -l`
        if [ "${NUM_FILES}" -eq "0" ]; then
            ERR_MSG="File(s) not found"
            # Log message
            echo "`date '+%F %H:%M:%S'` #ERROR ${ERR_MSG} for ${ob3M}'s ${file} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01" >> ${LOG_FILE} 2>&1
            # Control file on HDFS
            echo $ERR_MSG > ${TEMP_LOCAL_PATH}/.tmp_error
            hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${TEMP_LOCAL_PATH}/.tmp_error ${HDFS_PATH}/_ERROR
            rm -f ${TEMP_LOCAL_PATH}/.tmp_error
        else
            if hadoop fs -test -z ${HDFS_PATH}/_SUCCESS ; then
                # Log message
                #echo "`date '+%F %H:%M:%S'` #WARN  File ${HDFS_PATH} already exists. It will be skipped" >> ${LOG_FILE} 2>&1
                echo -e "file ${HDFS_PATH} already exists"
            else
            RAR_FILE_LIST=`ls ${RAR_FILE_GLOB} 2> /dev/null`
            echo $RAR_FILE_LIST > ${LOG_FILE} 2>&1
                ERRORS=0
                for RAR_FILE in ${RAR_FILE_LIST}; do
                    mkdir -p ${TEMP_LOCAL_PATH}
                    unrar e -o+ ${RAR_FILE} ${RAR_FILE_PATH} >> ${LOG_FILE} 2>&1
                    RAR_FILE_WITHOUT_PATH=$(basename $RAR_FILE)
                    DAT_FILE_WITHOUT_PATH=${RAR_FILE_WITHOUT_PATH/.rar/.TXT}
                    DAT_FILE=${RAR_FILE_PATH}${DAT_FILE_WITHOUT_PATH}
                    NUM_FILES=`ls ${DAT_FILE} 2> /dev/null | wc -l`
                    if [ "${NUM_FILES}" -eq "0" ]; then
                        ERR_MSG="File not found ${DAT_FILE_WITHOUT_PATH}"
                        # Log message
                        echo -e "`date '+%F %H:%M:%S'` #WARN  Check ${TEMP_LOCAL_PATH} and see why there's no file called ${DAT_FILE_WITHOUT_PATH}" >> ${LOG_FILE} 2>&1
                        echo -e "`date '+%F %H:%M:%S'` #ERROR ${ERR_MSG} for ${ob3M}'s ${file} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01" >> ${LOG_FILE} 2>&1
                        # Control file on HDFS
                        echo $ERR_MSG > ${TEMP_LOCAL_PATH}/.tmp_error
                        hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${TEMP_LOCAL_PATH}/.tmp_error ${HDFS_PATH}/_ERROR
                        rm -f ${TEMP_LOCAL_PATH}/.tmp_error
                        ERRORS=$((ERRORS + 1))
                        break
                    else
                        NUM_CABECERAS=`head -1 ${DAT_FILE} | sed 's/|/\n/g' | wc -l`
                        NUM_VALORES=`head -2 ${DAT_FILE} | tail -1 | sed 's/|/\n/g' | wc -l`
                        if [ "$NUM_CABECERAS" -eq "$NUM_VALORES" ]; then
                          echo "INFO  Cabeceras y valores coinciden ($NUM_CABECERAS)"
                        else
                          echo "ERROR Distinto numero de cabeceras ($NUM_CABECERAS) y valores ($NUM_VALORES). Se procesará igualmente."
                        fi
                        AWK_CODE="{ print ${COLUMNS[${file}]} }"
                        if [ ${file} != "TRAF_VOZ_HRC" ]; then
                          iconv -f Latin1 -t utf-8 ${DAT_FILE} | sed 's/\r//g' | cut -b 8-| tail -n+2 | gawk -F'|' "${AWK_CODE}" | sed 's/\"/\"\"/g' | gawk '{ print "\""$0"\"" }' | sed 's/|/\"|\"/g' > ${DAT_FILE/.TXT/.csv}
                        else
                          iconv -f Latin1 -t utf-8 ${DAT_FILE} | sed 's/\r//g' | tail -n+2 | gawk -F'|' "${AWK_CODE}" | sed 's/\"/\"\"/g' | gawk '{ print "\""$0"\"" }' | sed 's/|/\"|\"/g' > ${DAT_FILE/.TXT/.csv}
                        fi  
                        CSV_FILE=${DAT_FILE/.TXT/.csv}
                        CSV_FILE_WITHOUT_PATH=$(basename $CSV_FILE)
                        rm -fr ${DAT_FILE} > /dev/null 2>&1
                        echo -e "`date '+%F %H:%M:%S'` #INFO  Putting ${CSV_FILE_WITHOUT_PATH} file to HDFS" >> ${LOG_FILE} 2>&1
                        hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${CSV_FILE} ${HDFS_PATH}/${CSV_FILE_WITHOUT_PATH}
                        hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -touchz ${HDFS_PATH}/_SUCCESS
                        rm -fr ${CSV_FILE} > /dev/null 2>&1
                        
                        # Log message
                        echo -e "`date '+%F %H:%M:%S'` #INFO  File ${CSV_FILE} on ${CONTENT_YEAR}-${CONTENT_MONTH}-01 SUCCESSFULLY UPLOADED" >> ${LOG_FILE} 2>&1
                    fi
                done
                if [[ "${ERRORS}" == "0" ]]; then
                    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -touchz ${HDFS_PATH}/_SUCCESS
                fi
            fi
        fi
        echo "`date '+%F %H:%M:%S'` #INFO  ${file} processed" >> ${LOG_FILE} 2>&1
    done
    echo "`date '+%F %H:%M:%S'` #INFO  Finished processing files of ESP's LTV. Log file will be available on HDFS" >> ${LOG_FILE} 2>&1
    
    # BACKUP LOG TO HDFS
    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -mkdir -p ${LOGFILE_HDFS_PATH}
    hadoop fs -Dfs.permissions.umask-mode=${HDFS_UMASK} -put ${LOG_FILE} ${LOG_FILE_HDFS}
    
done
