#!/bin/bash

IGNITION_HOME=/home/search/search-ignition
LOG_DIR=/var/log/search-ignition
RESULTS_DIR=$LOG_DIR/results

mkdir -p $RESULTS_DIR
export LANG=C
export LC_ALL=C

nohup $IGNITION_HOME/scripts/job_runner.py search-etl $RESULTS_DIR >& $LOG_DIR/search_etl_job_runner.log &
