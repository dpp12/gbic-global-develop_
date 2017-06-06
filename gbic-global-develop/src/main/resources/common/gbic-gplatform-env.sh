#!/usr/bin/env bash

export GPLATFORM_HOME={{ remote.service }}

export DEFAULT_LOCAL_PATH_PREFIX="{{ remote.inbox }}"
#/user/gplatform/inbox/{OB3m}/MSv{VERS}/{DATASET}/month={YYYY]-{MM}-01/{OB2M}_{DATASET}_{YYYY}{MM}.bz2
export HDFS_INBOX="{{ hdfs.inbox }}"
export HDFS_SRVCHECKS="{{ hdfs.srvchecks }}"
export HDFS_TACS="{{ hdfs.tacs }}"
export HDFS_UMASK={{ hdfs.umask }}

DEFAULT_FILE_OB_PREFIX=""

# Oozie server
export OOZIE_URL={{ oozie.url }}

# Slack alarms
export INGEST_ALARM_URL="{{ slack.alarms['ingest'] }}"
export GENFILE_ALARM_URL="{{ slack.alarms['genfile'] }}"
export WORKFLOW_ALARM_URL="{{ slack.alarms['workflow'] }}"
export EXPORT_ALARM_URL="{{ slack.alarms['export'] }}"

{% if proxy.required %}
# Proxy settings
export http_proxy={{ proxy.http }}
export https_proxy={{ proxy.https }}
{% endif %}

# MySQL variables
export MYSQL_HOST="{{ db.host }}"
export MYSQL_PORT="{{ db.port }}"
export MYSQL_USER="{{ db.user }}"
export MYSQL_PASS="{{ db.pass }}"

# Service variables
export AREA="{{ project.area }}"
export SERVICE="{{ project.service }}"
export SERVICE_NAME="${AREA}-${SERVICE}"

# DEFAULT DATASET NAMES taken from ${GPLATFORM_HOME}/common/dataset-list.txt
export DEFAULT_DATASETS=$(while read -r __dataset; do
  echo "${__dataset} "
done < ${GPLATFORM_HOME}/common/dataset-list.txt)
