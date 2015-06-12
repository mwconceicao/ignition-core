#!/bin/bash


IGNITION_HOME=/home/search/search-ignition
LOG_DIR=/var/log/search-ignition
RESULTS_DIR=$LOG_DIR/results

mkdir -p $RESULTS_DIR
export LANG=C
export LC_ALL=C

nohup $IGNITION_HOME/scripts/job_runner.py sitemap-generation $RESULTS_DIR >& $LOG_DIR/sitemap_job_runner.log &

