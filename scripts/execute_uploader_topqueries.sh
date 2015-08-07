#!/bin/bash

IGNITION_HOME=/home/search/search-ignition

export LANG=C
export LC_ALL=C

cd $IGNITION_HOME
./sbt 'run-main ignition.jobs.utils.uploader.Uploader top-queries -s search-elb-esreport.chaordicsearch.com' >> /var/log/search-ignition/uploader-top-queries.log 2>&1