#!/bin/bash


IGNITION_HOME=/home/search/search-ignition
RESULTS_DIR=/var/log/search-ignition

mkdir -p $RESULTS_DIR
export LANG=C
export LC_ALL=C

nohup $IGNITION_HOME/scripts/job_runner.py sitemap-generation $RESULTS_DIR >& $IGNITION_HOME/sitemap_job_runner.log &

