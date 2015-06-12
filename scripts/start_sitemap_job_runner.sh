#!/bin/bash


IGNITION_HOME=/var/log/ignition-chaordic
RESULTS_DIR=$IGNITION_HOME/results

mkdir -p $RESULTS_DIR
export AWS_SECRET_ACCESS_KEY=$(s3cat s3://mail-keyring/ignition_access_key)
export AWS_ACCESS_KEY_ID=$(s3cat s3://mail-keyring/ignition_key_id)
export LANG=C
export LC_ALL=C

nohup $IGNITION_HOME/latest/job_runner.py sitemap-generation $RESULTS_DIR >& $IGNITION_HOME/sitemap_job_runner.log &

