#!/bin/bash

IGNITION_HOME=/home/search/search-ignition

export LANG=C
export LC_ALL=C

cd $IGNITION_HOME
./sbt 'run-main ignition.jobs.utils.uploader.Uploader valid-queries -s search-elb-esapi.chaordicsearch.com' >> /var/log/search-ignition/uploader-valid-queries.log 2>&1