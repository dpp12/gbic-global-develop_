#!/usr/bin/env bash
#
# calculate KPIs tariff dynamics B2B
# ----------------------------------
# 
# Usage:
# 
# [hdfs]# nohup ./calculate_kpis_tariff_dynamics_b2b.sh &
# 
# Monitor with...: sudo -u hdfs hive -e "use gbic_global; show partitions gbic_global_kpis_tariff_dynamics_agg_b2b" 2>/dev/null
# and............: sudo tail -F /home/hdfs/calculate_kpis_tariff_dynamics_b2b.log
# 
###################################################################################################


# ARGS: none
###################################################################################################
execTime=`date '+%F %H:%M:%S'`


# CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
LOGFILE=calculate_kpis_tariff_dynamics_b2b.log
RESULTFILE=calculate_kpis_tariff_dynamics_b2b

OBS="
      1
      2
      3
    201
"
DATES="
    2015-01-01
    2015-02-01
    2015-03-01
    2015-04-01
    2015-05-01
    2015-06-01
    2015-07-01
    2015-08-01
"


# INITIALIZATION
###################################################################################################


# PROCESS
###################################################################################################
hive -f calculate_kpis_tariff_dynamics_b2b_DDL.hql >> ${LOGFILE}

for ob in ${OBS}; do
    
    echo -e "\n[${ob}] START" >> ${LOGFILE}
    
    for curr in ${DATES}; do
        
        echo -e "[${ob}][${curr}] begin month" >> ${LOGFILE}
        
        prev=$(date "--date=${curr} -1 month" +"%Y-%m-%d")
        
        hive \
            -hiveconf ob=${ob} \
            -hiveconf curdate=${curr} \
            -hiveconf prevdate=${prev} \
            -f calculate_kpis_tariff_dynamics_b2b_churner.hql 1> ${RESULTFILE}.${ob}.${curr}.log 2>> ${LOGFILE}
        
        hive \
            -hiveconf ob=${ob} \
            -hiveconf curdate=${curr} \
            -hiveconf prevdate=${prev} \
            -f calculate_kpis_tariff_dynamics_b2b_repo.hql 1>> ${RESULTFILE}.${ob}.${curr}.log 2>> ${LOGFILE}
        
    done
    
done
