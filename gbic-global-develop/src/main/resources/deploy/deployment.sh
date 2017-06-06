#!/usr/bin/env bash
# 
# deployment
# ----------
# 
# Deploys GPlatform Global Project software in the specified environment
# 
# Usage:
# 
# $ ./deployment.sh [OPTION] ENVIRONMENT
# 
# Example:
# 
# $ ./deployment.sh --start-date=2015-02-01 dev
# 
###################################################################################################


# FUNCTIONS AND CONSTANTS FOR SERVICE CONFIGURATION
###################################################################################################
THE_SERVICE="GLOBAL GPLATFORM"
showHelp () {
  echo -e "Usage: $0 [OPTION] ENVIRONMENT"
  echo -e "Deploys ${THE_SERVICE} Project Software to the specified environment based on predefined config files\n"
  echo -e "\t-c ENABLE, --custom-deployment=ENABLE"
  echo -e "\t               If the installation is done in a custom folder or in the definitive one."
  echo -e "\t               'enabled' by default."
  echo -e "\t               Set to 'disabled' to force productive installation:"
  echo -e "\t                 --custom-deployment=disabled"
  echo -e "\t               When custom deployment is disabled, ansible will prompt user for password."
  echo -e "\t-u USER, --ssh-user=USER"
  echo -e "\t               Remote user, for the deployment and execution of scripts."
  echo -e "\t               If not received, will look for SSH_USER variable in the environment and,"
  echo -e "\t               if not found, will use USER environment variable"
  echo -e "\t               (the user executing ansible locally)."
  echo -e "\t-s DATE, --start-date=DATE"
  echo -e "\t               Oozie coordinator start date."
  echo -e "\t               If not specified, current day will be used."
  echo -e "\t               If desired first month is Jan-2016 use 2016-02-01."
  echo -e "\t-e DATE, --end-date=DATE"
  echo -e "\t               Oozie coordinator end date."
  echo -e "\t               If not specified, 2099-12-31 will be used."
  echo -e "\t               If desired last month is Jan-2016 use 2016-03-01."
  echo -e "\t-o ACTION, --oozie-action=ACTION"
  echo -e "\t               Oozie action to execute."
  echo -e "\t               Possible values are:"
  echo -e "\t                 - none"
  echo -e "\t                 - submit\t[by default]"
  echo -e "\t                 - run"
  echo -e "\t-n, --no-mvn"
  echo -e "\t               Flag to skip maven generation of artifacts."
  echo -e "\t               If not present two actions will be executed prior to deployment:"
  echo -e "\t                 - mvn clean package -DskipTests"
  echo -e "\t                 - mvn dependency:copy-dependencies -Dsilent=true -DincludeScope=runtime"
  echo -e "\t               Use flag to disable maven actions,"
  echo -e "\t               when generation is managed outside of (and before) this script."
  echo -e "\t-a, --alarm-url"
  echo -e "\t               URL of the Slack webhook used to send alarms when deployment fails."
  echo -e "\t                 - https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/xxxxxxxxxxxxxxxxxxxxxxxx"
  echo -e "\nRecognized ENVIRONMENT format:"
  echo -e "\tLowercase names defined in deploy/envs folder of project. Examples: dev-pre, dev, qa, pro"
  echo -e "\nRecognized DATE format:"
  echo -e "\tyyyy-MM-DD"
}

SCRIPT_PATH=`readlink -e $0`
SCRIPT_PATH=`dirname ${SCRIPT_PATH}`
PROJ_HOME=`cd "${SCRIPT_PATH}/../../../.."; pwd`

source ${PROJ_HOME}/src/main/resources/common/gbic-gplatform-common.sh

PLAYBOOKS_PATH=${SCRIPT_PATH}/common/playbooks
ENVIRONMENTS_PATH=${SCRIPT_PATH}/envs

VALID_ENVIRONMENT_LIST=`ls ${ENVIRONMENTS_PATH}`
VALID_OOZIE_ACTIONS="run submit none"
DEFAULT_OOZIE_END_DATE='2099-12-31'


# Finalizes script with an error code and an alarm. It receives two arguments:
# - ERROR_CODE: numeric code of the error for ending the script
# - ERROR_MESSAGE: descriptive text of the error that caused the finalization of script
# Example:
#   die 27 "There is not connection to server"
die () {
  __die "${ALARM_URL}"    \
        ":ansible:"       \
        "$1"              \
        "$2"              \
        "${THE_SERVICE}"  \
        "Deployment of ${THE_SERVICE} on '${ENVIRONMENT}' environment"
}


# OPTIONS: -c ENABLE, -u USER, -s DATE, -e DATE, -o ACTION, -n, -a URL
###################################################################################################
CUSTOM=
ALT_USER=
OOZIE_START=
OOZIE_END=
OOZIE_ACTION=
OPT_NO_MVN=0
ALARM_URL=

while getopts '\-:-c:-u:-s:-e:-o:n-a:' opt; do
  case ${opt} in
    - )
      long_optarg="${OPTARG#*=}"
      case "${OPTARG}" in
        custom-deployment=?* ) CUSTOM=${long_optarg};;
        custom-deployment*   ) cancel "option requires an argument -- ${OPTARG}";;
        ssh-user=?*          ) ALT_USER=${long_optarg};;
        ssh-user*            ) cancel "option requires an argument -- ${OPTARG}";;
        start-date=?*        ) OOZIE_START=${long_optarg};;
        start-date*          ) cancel "option requires an argument -- ${OPTARG}";;
        end-date=?*          ) OOZIE_END=${long_optarg};;
        end-date*            ) cancel "option requires an argument -- ${OPTARG}";;
        oozie-action=?*      ) OOZIE_ACTION=${long_optarg};;
        oozie-action*        ) cancel "option requires an argument -- ${OPTARG}";;
        no-mvn               ) OPT_NO_MVN=1;;
        no-mvn*              ) cancel "option doesn't allow an argument -- ${OPTARG}";;
        alarm-url=?*         ) ALARM_URL=${long_optarg};;
        alarm-url*           ) cancel "option requires an argument -- ${OPTARG}";;
        help                 ) help;;
        # "--" terminates argument processing
        ''                   ) break;;
        *                    ) cancel "illegal option -- ${OPTARG}";;
      esac
      ;;
    c ) CUSTOM=${OPTARG};;
    u ) ALT_USER=${OPTARG};;
    s ) OOZIE_START=${OPTARG};;
    e ) OOZIE_END=${OPTARG};;
    o ) OOZIE_ACTION=${OPTARG};;
    n ) OPT_NO_MVN=1;;
    a ) ALARM_URL=${OPTARG};;
    \?) cancel;;
  esac
done
shift $((--OPTIND))

# -------------------------------------------------------------------------------------------------
# Validate CUSTOM
# -------------------------------------------------------------------------------------------------
CUSTOM_DEPLOYMENT="1"
if [[ "${CUSTOM}" == "disabled" ]]; then
  CUSTOM_DEPLOYMENT="0"
fi
# -------------------------------------------------------------------------------------------------
# Validate ALT_USER
# -------------------------------------------------------------------------------------------------
if [[ "${ALT_USER}" != "" ]]; then
  REMOTE_USER=${ALT_USER}
elif [ -z "${SSH_USER}" ]; then
  REMOTE_USER=${USER}
else
  REMOTE_USER=${SSH_USER}
fi
# -------------------------------------------------------------------------------------------------
# Validate OOZIE_ACTION
# -------------------------------------------------------------------------------------------------
ACTION=${OOZIE_ACTION}
validAction=0
if [[ "${ACTION}" != "" ]]; then
  for action in ${VALID_OOZIE_ACTIONS}; do
    if [[ "${ACTION}" == "${action}" ]]; then
      validAction=1
      break
    fi
  done
fi
if [ ${validAction} -eq 0 ]; then
  ACTION=submit
fi
# -------------------------------------------------------------------------------------------------
# Validate OPT_NO_MVN
# -------------------------------------------------------------------------------------------------
# --no-mvn flag is mandatory if MAVEN_HOME is not set
if [[ "${OPT_NO_MVN}" == "0" ]]; then
  if [ -z "${MAVEN_HOME}" ]; then
    cancel "MAVEN_HOME variable is not set. Please set to a valid path or run script with --no-mvn flag to skip artifact generation."
  else
    MAVEN_VERSION=`${MAVEN_HOME}/bin/mvn --version 2>/dev/null | head -1 | gawk '{ print $3 }'`
    if [[ "${MAVEN_VERSION}" == "" ]]; then
      cancel "Unable to execute mvn from ${MAVEN_HOME}. Please check value of MAVEN_HOME variable."
    fi
  fi
fi


# ARGS: ENVIRONMENT
###################################################################################################
# Validate ENVIRONMENT
# -------------------------------------------------------------------------------------------------
ENVIRONMENT=$1
if [[ "${ENVIRONMENT}" == "" ]]; then
  cancel "wrong ENVIRONMENT for deploying software to: NOT RECEIVED"
else
  validEnv=0
  validEnvs=`echo ${VALID_ENVIRONMENT_LIST}`
  for env in ${validEnvs}; do
    if [[ "${ENVIRONMENT}" == "${env}" ]]; then
      validEnv=1
      break
    fi
  done
  if [ ${validEnv} -eq 0 ]; then
    cancel "wrong ENVIRONMENT for deploying software to: '${ENVIRONMENT}' NOT AVAILABLE. Please, choose one of the following: { ${validEnvs//[ ]/, } }."
  fi
fi


# INITIALIZATION
###################################################################################################
EXEC_TIME=`date '+%F %H:%M:%S'`
TIMESTAMP=`date '+%Y%m%d%s'`
LOAD_DATE=`date '+%Y-%m-%d'`

# CUSTOM DEPLOYMENT
CLIENT_BASE=
HDFS_BASE=
CUSTOM_PREFIX=
ASK_PASS=
if [[ "${CUSTOM_DEPLOYMENT}" == "1" ]]; then
  # There where the home of a user was not /home/<name> custom deployment won't be possible
  CLIENT_BASE="/home/${REMOTE_USER}"
  HDFS_BASE="/user/${REMOTE_USER}"
  CUSTOM_PREFIX="${REMOTE_USER}_"
else
  # In NON CUSTOM deployments, script will prompt for password
  ASK_PASS="--ask-pass"
fi

# OOZIE START DATE
if [[ "${OOZIE_START}" == "" ]]; then
  START_DATE=${LOAD_DATE}
else
  START_DATE=${OOZIE_START}
fi

# OOZIE INITIAL DATE
INITIAL_DATE=$(date "--date=${START_DATE} -1 day" +"%Y-%m-%d")

# OOZIE END DATE
if [[ "${OOZIE_END}" == "" ]]; then
  END_DATE=${DEFAULT_OOZIE_END_DATE}
else
  END_DATE=${OOZIE_END}
fi


# MAIN PROCESS
###################################################################################################

# -------------------------------------------------------------------------------------------------
# Some verbosity
# -------------------------------------------------------------------------------------------------
# TODO if OPTION -v show all this info, else don't
info "Launched ${THE_SERVICE} Project Software Deployment Script on ${EXEC_TIME}"
info "CONFIG:"
info "|- PROJECT_HOME=${PROJ_HOME}"
info "|- USER=${USER}"
info "OPTIONS:"
info "|- Run for REMOTE_USER '${REMOTE_USER}'"
if [[ "${ALT_USER}" != "" ]]; then
  info "|  > Specified with -u, --ssh-user: ${ALT_USER}"
elif [[ "${SSH_USER}" != "" ]]; then
  info "|  > Specified by env: SSH_USER=${SSH_USER}"
else
  info "|  > By default. Current user: ${USER}"
fi
if [[ "${CUSTOM}" == "disabled" ]]; then
  info "|- Run with NO CUSTOM deployment"
else
  info "|- Run with CUSTOM deployment"
  info "|  > Client installation Base path....: ${CLIENT_BASE}"
  info "|  > Cluster installation Base path...: ${HDFS_BASE}"
fi
info "|- Oozie configuration:"
if [[ "${OOZIE_START}" == "" ]]; then
  info "|  > Oozie Start Date not specified. Default date will be used (current day '${START_DATE}')"
else
  info "|  > Oozie Start Date: '${START_DATE}'"
fi
info "|  > Oozie Initial Date (the day before the 'Start Date'): '${INITIAL_DATE}'"
if [[ "${OOZIE_END}" == "" ]]; then
  info "|  > Oozie End Date not specified. Default date will be used ('${END_DATE}')"
else
  info "|  > Oozie End Date: '${END_DATE}'"
fi
if [[ "${validAction}" == "0" ]]; then
  if [[ "${OOZIE_ACTION}" == "" ]]; then
    info "|  > Oozie action not set. Default action will be used: '${ACTION}'"
  else
    info "|  > Oozie action '${OOZIE_ACTION}' not recognized. Expected one of { ${VALID_OOZIE_ACTIONS//[ ]/, } }. Default will be used: '${ACTION}'"
  fi
else
  info "|  > Oozie action set to '${ACTION}'"
fi
if [[ "${OPT_NO_MVN}" == "1" ]]; then
  info "|- Artifacts won't be generated using Maven, but they will be deployed anyway"
else
  info "|- Artifacts will be generated using maven version ${MAVEN_VERSION}"
  info "|  > from MAVEN_HOME=${MAVEN_HOME}"
fi
if [[ "${ALARM_URL}" == "" ]]; then
  info "|- Don't send Slack Alerts"
else
  info "|- Send Slack ALERTS using webhook URL: ${ALARM_URL}"
fi
info "ARGUMENTS:"
info "|- Environment for deploying software to: '${ENVIRONMENT}'"

# -------------------------------------------------------------------------------------------------
# Generate artifact
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_NO_MVN}" == "0" ]]; then
  info "Generating project artifact..."
  cd ${PROJ_HOME}
  ${MAVEN_HOME}/bin/mvn clean package -DskipTests
  
  exitCode=$?
  cd - > /dev/null
  if [[ "${exitCode}" != "0" ]]; then
    die 2 "mvn artifact generation failed with exit code ${exitCode}"
  fi
  info "Finished compilation"
fi

# -------------------------------------------------------------------------------------------------
# Get project dependencies
# -------------------------------------------------------------------------------------------------
if [[ "${OPT_NO_MVN}" == "0" ]]; then
  info "Getting project runtime dependencies..."
  cd ${PROJ_HOME}
  ${MAVEN_HOME}/bin/mvn dependency:copy-dependencies -Dsilent=true -DincludeScope=runtime
  
  exitCode=$?
  cd - > /dev/null
  if [[ "${exitCode}" != "0" ]]; then
    die 3 "mvn dependencies obtaining failed with exit code ${exitCode}"
  fi
  info "Finished obtaining dependencies"
fi

# -------------------------------------------------------------------------------------------------
# Execute Ansible playbook
# -------------------------------------------------------------------------------------------------
info "Running ansible playbook..."
ansible-playbook ${PLAYBOOKS_PATH}/main.yml \
                 ${ASK_PASS}                     \
                 -e "projecthome=${PROJ_HOME}    \
                     clientbase=${CLIENT_BASE}   \
                     hdfsbase=${HDFS_BASE}       \
                     prefix=${CUSTOM_PREFIX}     \
                     initialdate=${INITIAL_DATE} \
                     startdate=${START_DATE}     \
                     enddate=${END_DATE}         \
                     oozieaction=${ACTION}       \
                     timestamp=${TIMESTAMP}      \
                     deployenv=${ENVIRONMENT}    \
                     remoteuser=${REMOTE_USER}"  \
                 -i ${ENVIRONMENTS_PATH}/${ENVIRONMENT}/inventory.ini

exitCode=$?
if [[ "${exitCode}" != "0" ]]; then
  die 4 "ansible-playbook failed with exit code ${exitCode}"
fi
info "Finished deployment"

# -------------------------------------------------------------------------------------------------
# TODO if OPTION --notify='@miqui' -> Notify @miqui
info "Finished process. SUCCESS!"
