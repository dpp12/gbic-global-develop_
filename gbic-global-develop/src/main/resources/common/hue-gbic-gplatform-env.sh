#!/usr/bin/env bash

# Use this file to override values that should be different when executing scripts via HUE's Job Designer

# local-gbic-gplatform-env.sh does not exist until runtime, and it is original gbic-gplatform-env.sh
# when executed via HUE, while hue-gbic-gplatform-env.sh becomes new gbic-gplatform-env.sh
source ./common/local-gbic-gplatform-env.sh

export GPLATFORM_HOME=.

# DEFAULT DATASET NAMES taken from ${GPLATFORM_HOME}/common/dataset-list.txt
export DEFAULT_DATASETS=$(while read -r __dataset; do
  echo "${__dataset} "
done < ${GPLATFORM_HOME}/common/dataset-list.txt)
