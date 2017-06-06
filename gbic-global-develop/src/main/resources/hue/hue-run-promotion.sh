#!/usr/bin/env bash
# 
# hue endpoint: run promotion process
# -----------------------------------
# 
# Proxy script to build an endpoint in hue for the execution of the run-promotion process.
# It would be only invocated from hue job-designer tool.
#
# # ${SCRIPTS_HOME}/hue-run-promotion.sh {vers-num} {country} {dataset} [{yyyyMM}]
# 
###################################################################################################

# Download required scripts to local in worker node
hdfs dfs -get {{ cluster.service }}/common
hdfs dfs -get {{ cluster.service }}/workflow
# Remove version level in local path
mkdir -p etl/scripts
hdfs dfs -get {{ cluster.service }}/etl/scripts/{{ project.version }}/* etl/scripts

# Switch ENV files to use HUE ones
mv common/gbic-gplatform-env.sh common/local-gbic-gplatform-env.sh
mv common/hue-gbic-gplatform-env.sh common/gbic-gplatform-env.sh

# Set execution permissions on scripts
chmod u+x workflow/run-promotion.sh
chmod u+x etl/scripts/*/*HivePromotion.hql

# Execute run-promotion script
./workflow/run-promotion.sh $1 $2 $3 $4
