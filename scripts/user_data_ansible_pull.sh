#!/bin/bash -ex

# Override default Spark variables
echo "export SPARK_WORKER_DIR='/mnt/root/spark/work'" >> /etc/environment

exit 0
