#!/usr/bin/env bash
# 
# gplatform insert process
# ------------------------
# 
# Takes actual date and startdate to insert homogenization segments into hdfs. 
# It has to be executed as 'javierb' user.
# 
# Usage: (as javierb)
# 
# # /opt/gbic/services/gplatform/global/mysql/std_model/std_model_loading.sh
# 
# Example:
# 
# # /opt/gbic/services/gplatform/global/mysql/std_model/std_model_loading.sh
# 
###################################################################################################
current_date=$(date +%Y%m01)
date="20150101"
while [[ $date -le $current_date ]]; do
     month=$(date --date "$date" +%Y%m)
     {{ remote.service }}/etl/scripts/std_dim_download.sh $month
     date=$(date "--date=${date} +1 month" +"%Y%m%d")
done
