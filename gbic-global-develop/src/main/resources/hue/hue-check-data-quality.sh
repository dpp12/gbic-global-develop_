#!/usr/bin/env bash
# 
# hue endpoint: check data quality process
# ----------------------------------------
# 
# Proxy script to build an endpoint in hue for the execution of the check-data-quality process.
# It would be only invocated from hue job-designer tool.
#
# # ${SCRIPTS_HOME}/hue-check-data-quality.sh {vers-num} {country} {dataset} [{yyyyMM}]
# 
###################################################################################################

# Download required scripts to local in worker node
hdfs dfs -get {{ cluster.service }}/common
hdfs dfs -get {{ cluster.service }}/etl
hdfs dfs -get {{ cluster.service }}/workflow

# Switch ENV files to use HUE ones
mv common/gbic-gplatform-env.sh common/local-gbic-gplatform-env.sh
mv common/hue-gbic-gplatform-env.sh common/gbic-gplatform-env.sh

# Set execution permissions on scripts
chmod u+x workflow/check-data-quality.sh

# Execute run-promotion script
./workflow/check-data-quality.sh $1 $2 $3
