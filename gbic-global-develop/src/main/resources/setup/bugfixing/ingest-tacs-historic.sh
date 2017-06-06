#!/usr/bin/env bash
# 
# gplatform insert process
# ------------------------
# 
# Takes actual date and startdate to insert devices catalog into hdfs. 
# It has to be executed as 'admin' user.
# 
# Usage: (as admin)
# 
# # /opt/gbic/services/gplatform/global/setup/bugfixing/ingest-tacs-historic.sh
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/setup/bugfixing/ingest-tacs-historic.sh
# 
###################################################################################################
GBIC_HOME=`readlink -e $0`
GBIC_HOME=`dirname ${GBIC_HOME}`
GBIC_HOME=`cd "${GBIC_HOME}/../.."; pwd`

source ${GBIC_HOME}/common/gbic-gplatform-common.sh
source ${GBIC_HOME}/common/gbic-gplatform-env.sh

current_date=$(date +%Y%m01)
date="20150101"
while [[ $date -le $current_date ]]; do
     month=$(date --date "$date" +%Y%m)
     ${GPLATFORM_HOME}/ingestion/ingest-tacs.sh $month ${DEFAULT_LOCAL_PATH_PREFIX}
     date=$(date "--date=${date} +1 month" +"%Y%m%d")
     cp ${DEFAULT_LOCAL_PATH_PREFIX}/LTV/LTVtoGBIC/DEVICES_CATALOG/${month}/SF_ExportCatalogo_GBI.txt.gz ${DEFAULT_LOCAL_PATH_PREFIX}/LTV/LTVtoGBIC/.
done
