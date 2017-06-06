#!/usr/bin/env bash

jobId=$(oozie jobs -filter name=$1\;status=PREP\;status=RUNNING -jobtype COORDINATOR| grep -oh '.*-C')
if [ ! -z ${jobId} ]; then
   oozie job -kill ${jobId} -oozie $2
fi
