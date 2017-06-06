#!/usr/bin/env bash
# 
# Pricing Datamart building
# ---------------------------
# 
# Script that removes and creates GBIC_GLOBAL_DM_PRICING database from hive, 
# Optionally extracts a FWD and BWD samples and recalculates the dataset for the new samples
# 
# Usage:
# 
# # /opt/gbic/services/global/datamarts/pricing/build_dm_pricing.sh [{OB}] [sample] [{n_pre} {n_pos} {h_hyb}]
# 
#   - {OB}: One of {'ES', 'BR', 'AR', 'CL'}. If not specified, all of them will be taken.
#   - sample: Flag to regenerate sample with default numbers
#   - {n_pre}, {n_pos}, {n_hyb}: Number of records to generate for each kind of service (Prepaid, Contract, Hybrid). Won't be read if {OB} isn't present.
# 
# Examples:
# 
# ## If you want to generate the dataset for all OBs you should type NO arguments
# # /opt/gbic/services/global/datamarts/pricing/build_dm_pricing.sh
# 
# ## If you want to generate the sample and dataset for all OBs you should tye one argument
# # /opt/gbic/services/global/datamarts/pricing/build_dm_pricing.sh sample
# 
# ## If you want to generate only the dataset for an OB you should type only one argument: the ob (it allows a space separated list netween quotes)
# # /opt/gbic/services/global/datamarts/pricing/build_dm_pricing.sh AR
# 
# ## If you want to generate the sample and dataset for an OB you should type two arguments: the ob and the word sample
# # /opt/gbic/services/global/datamarts/pricing/build_dm_pricing.sh AR sample
# 
# ## If you want to generate the prepaid contract or hybrid sample and dataset for an OB and only for prepaid or contract or hybrid
# ## you should type three arguments: the ob, the word sample and the number of prepaid lines for the sample and Number of contract lines for the sample
# ## and number of Hybrid lines for the sample
# # /opt/gbic/services/global/datamarts/pricing/build_dm_pricing.sh ARG sample 1000 0 0
# 
###################################################################################################


# DEFAULT VALUES
###################################################################################################
DEFAULT_OBS_2M="ES BR AR CL"
DEFAULT_PREPAID_SAMPLE_LINES=100000
DEFAULT_CONTRACT_SAMPLE_LINES=25000
DEFAULT_HYBRID_SAMPLE_LINES=25000

# Can be configured to generate only from one specific month
INITIAL_MONTH='2015-01-01'
FINAL_MONTH='9999-12-31'


# ARGS: OBS_3M, SAMPLE_REGENERATION_FLAG, NUMBER_OF_PREPAID_LINES, NUMBER_OF_CONTRACT_LINES, NUMBER_OF_HYBRID_LINES (all optional)
###################################################################################################
execTime=`date '+%F %H:%M:%S'`

# Console message
echo -e "`date '+%F %H:%M:%S'` #INFO  Launched Pricing DM generation script on ${execTime}"

# Execution with no args, or with only SAMPLE_GENERATION_FLAG will use the default OB list.
if [[ "$1" == "" || "$1" == "sample" ]]; then
    OBS_2M=${DEFAULT_OBS_2M}
    # Console message
    echo -e "`date '+%F %H:%M:%S'` #INFO  |- Country list not received. All countries will be refreshed: ${OBS_2M//[ ]/, }."
else
    OBS_2M=$1
    # Console message
    echo -e "`date '+%F %H:%M:%S'` #INFO  |- Countries to be refreshed: ${OBS_2M//[ ]/, }."
fi

# By default, SAMPLE will NOT be regenerated. Only will do if SAMPLE_REGENERATION_FLAG is used (with or without previous OB list)
# Optionally, when called for a specific list of OBs, it can be specified the numbers for the three samples (But if one specified, the three are required)
if [[ "$1" == "sample" || "$2" == "sample" ]]; then
    CREATE_SAMPLE=1
    # Console message
    echo -e "`date '+%F %H:%M:%S'` #INFO  |- Sample will be regenerated."
    if [[ "$2" == "sample" && "$3" != "" && "$4" != "" && "$5" != "" ]]; then
        # Console messages
        echo -e "`date '+%F %H:%M:%S'` #INFO     Number of lines specified for each type of service:"
        PREPAID_SAMPLE_LINES=$3
        CONTRACT_SAMPLE_LINES=$4
        HYBRID_SAMPLE_LINES=$5
    else
        # Console messages
        echo -e "`date '+%F %H:%M:%S'` #INFO     Number of lines NOT specified for each type of service. Default values will be used:"
        PREPAID_SAMPLE_LINES=${DEFAULT_PREPAID_SAMPLE_LINES}
        CONTRACT_SAMPLE_LINES=${DEFAULT_CONTRACT_SAMPLE_LINES}
        HYBRID_SAMPLE_LINES=${DEFAULT_HYBRID_SAMPLE_LINES}
    fi
    echo -e "`date '+%F %H:%M:%S'` #INFO     |- Lines for PREPAID....: ${PREPAID_SAMPLE_LINES}"
    echo -e "`date '+%F %H:%M:%S'` #INFO     |- Lines for CONTRACT...: ${CONTRACT_SAMPLE_LINES}"
    echo -e "`date '+%F %H:%M:%S'` #INFO     |- Lines for HYBRID.....: ${HYBRID_SAMPLE_LINES}"
else
    CREATE_SAMPLE=0
    # Console message
    echo -e "`date '+%F %H:%M:%S'` #INFO  |- Sample will NOT be regenerated."
    PREPAID_SAMPLE_LINES=0
    CONTRACT_SAMPLE_LINES=0
    HYBRID_SAMPLE_LINES=0
fi


# CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################

getIndexOB() {
    case $1 in
      'ES') return 0;;
      'BR') return 1;;
      'AR') return 2;;
      'CL') return 3;;
      'PE') return 4;;
         *) return 99;;
    esac
}
OBS_3M=(ESP BRA ARG CHL PER)
OBS_op=(  1 201   2   3   5)


# INITIALIZATION
###################################################################################################
# Validate OB list
for ob2M in ${OBS_2M}; do
    getIndexOB ${ob2M}; indexOB=$?
    if [[ "$indexOB" = "99" ]]; then
        echo -e "`date '+%F %H:%M:%S'` #ERROR OB '${ob2M}' not available. Please, choose one of the following: ${DEFAULT_OBS_2M//[ ]/, }."
        echo -e "`date '+%F %H:%M:%S'` #FATAL Program will exit now."
        exit 1
    fi
done


# GENERATE DATASETS
###################################################################################################

# Creation of database if not exists
hive -f hive_creation.sql

# Execution of dataset for every OB
for ob2M in ${OBS_2M}; do
    
    getIndexOB ${ob2M}; indexOB=$?
    ob3M=${OBS_3M[$indexOB]}
    opid=${OBS_op[$indexOB]}
    
    if [ "${CREATE_SAMPLE}" -eq "1" ]; then
        
        # -----------------------------------------------------------------------------------------
        # SAMPLE GENERATION
        # -----------------------------------------------------------------------------------------
        echo -e "`date '+%F %H:%M:%S'` #INFO  Calculating BWD sample for gbic_op_id=${opid} (${ob3M})."
        hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict \
             --hivevar targetOb=${opid}                            \
             --hivevar sampleType='BWD'                            \
             --hivevar pLimit=${PREPAID_SAMPLE_LINES}              \
             --hivevar cLimit=${CONTRACT_SAMPLE_LINES}             \
             --hivevar hLimit=${HYBRID_SAMPLE_LINES}               \
             -f ./dm_pricing_sample.sql
        
        echo -e "`date '+%F %H:%M:%S'` #INFO  Calculating FWD sample for gbic_op_id=${opid} (${ob3M})."
        hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict \
             --hivevar targetOb=${opid}                            \
             --hivevar sampleType='FWD'                            \
             --hivevar pLimit=${PREPAID_SAMPLE_LINES}              \
             --hivevar cLimit=${CONTRACT_SAMPLE_LINES}             \
             --hivevar hLimit=${HYBRID_SAMPLE_LINES}               \
             -f ./dm_pricing_sample.sql
        # -----------------------------------------------------------------------------------------
    fi
    
    # -----------------------------------------------------------------------------------------
    # DATASET REFRESH
    # -----------------------------------------------------------------------------------------
    echo -e "`date '+%F %H:%M:%S'` #INFO  Calculating dataset for gbic_op_id={opid} (${ob3M})."
    hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict \
         --hiveconf hive.auto.convert.join=false               \
         --hivevar targetOb=${opid}                            \
         --hivevar initial_month="${INITIAL_MONTH}"            \
         --hivevar final_month="${FINAL_MONTH}"                \
         -f ./dm_pricing_data.sql
    
    echo -e "`date '+%F %H:%M:%S'` #INFO  Calculating dataset from daily traffic for gbic_op_id ${opid} (${ob3M})."
    hive --hiveconf hive.exec.dynamic.partition.mode=nonstrict \
         --hivevar targetOb=${opid}                            \
         --hivevar initial_month="${INITIAL_MONTH}"            \
         --hivevar final_month="${FINAL_MONTH}"                \
         -f ./dm_pricing_data_daily_traffic.sql
    # -----------------------------------------------------------------------------------------
    
    echo -e "`date '+%F %H:%M:%S'` #INFO  Finished working on dataset for gbic_op_id ${opid} (${ob3M})."
done

echo -e "`date '+%F %H:%M:%S'` #INFO  All work is done. SUCCESS!"
