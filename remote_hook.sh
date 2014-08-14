#!/bin/bash

# We suppose we are in a subdirectory of the root project
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

JOB_NAME="${1:?Please give a Job Name}"
JOB_DATE="${2?Please give the Job Date}"
JOB_TAG="${3?Please give the Job Tag}"
JOB_USER="${4?Please give the Job User}"
CONTROL_DIR="${5?Please give the Control Directory}"
SPARK_MEM_PARAM="${6?Please give the Job Memory Size to use}"
USE_YARN="${7?Please tell if we should use YARN (yes/no)}"
NOTIFY_ON_ERRORS="${8?Please tell if we will notify on errors (yes/no)}"

JOB_WITH_TAG=${JOB_NAME}.${JOB_TAG}
JOB_CONTROL_DIR="${CONTROL_DIR}/${JOB_WITH_TAG}"
mkdir -p "${JOB_CONTROL_DIR}"

RUNNING_FILE="${JOB_CONTROL_DIR}/RUNNING"
echo $$ > "${RUNNING_FILE}"

notify_error_and_exit() {
    description="${1}"
    echo "Exiting because: ${description}"
    echo "${description}" > "${JOB_CONTROL_DIR}/FAILURE"
    # Do not notify if NOTIFY_ON_ERRORS is different from yes
    [[ "${NOTIFY_ON_ERRORS}" != 'yes' ]] && exit 1
    # TODO: create some generic method to notify on errors
    exit 1
}

get_first_present() {
    for dir in "$@"; do
        if [[ -e "$dir" ]]; then
            echo $dir
            break
        fi
    done
}

on_trap_exit() {
    rm -f "${JOB_CONTROL_DIR}"/*.jar
    rm -f "${RUNNING_FILE}"
}


trap "on_trap_exit" EXIT

SPARK_HOME=$(get_first_present /root/spark /opt/spark ~/spark*/)
source "${SPARK_HOME}/conf/spark-env.sh"

MAIN_CLASS="ignition.jobs.Runner"

basic_job_opts="-Dspark.logConf=true -Dspark.default.parallelism=640 -Dspark.akka.frameSize=15 -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps -Dspark.speculation=true"
#kryo_opts="-Dspark.serializer=org.apache.spark.serializer.KryoSerializer -Dspark.kryoserializer.buffer.mb=20"
compress_opts="-Dspark.hadoop.mapreduce.output.fileoutputformat.compress=true -Dspark.hadoop.mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec -Dspark.hadoop.mapreduce.output.fileoutputformat.compress.type=BLOCK"
low_memory_opts="-Dspark.shuffle.memoryFraction=0.3 -Dspark.storage.memoryFraction=0.1 -Dspark.reducer.maxMbInFlight=10 -XX:-UseGCOverheadLimit"
#cores_option="-Dspark.cores.max=320"

job_opts="$basic_job_opts $low_memory_opts $compress_opts $kryo_opts $cores_option"

cd "${DIR}" || notify_error_and_exit "Internal script error for job ${JOB_WITH_TAG}"
./sbt assembly || notify_error_and_exit "Failed to build job assembly for job ${JOB_WITH_TAG}"

JAR_PATH_SRC=$(echo "${DIR}"/target/scala-*/*assembly*.jar)
JAR_PATH="${JOB_CONTROL_DIR}/Ignition.jar"

cp ${JAR_PATH_SRC} ${JAR_PATH}

export SPARK_MEM=${SPARK_MEM_PARAM}
export JOB_MASTER=${MASTER}

if [[ "${USE_YARN}" == "yes" ]]; then
    export YARN_MODE=true
    export JOB_MASTER="yarn-client"
    export SPARK_JAR=$(echo ${SPARK_HOME}/assembly/target/scala-*/spark-assembly-*.jar)
    export SPARK_YARN_APP_JAR=${JAR_PATH}
    export SPARK_WORKER_MEMORY=${SPARK_MEM}
fi


if [[ "${JOB_NAME}" == "shell" ]]; then
    export ADD_JARS="${JAR_PATH}"
    export SPARK_REPL_OPTS="$job_opts"
    sudo -E ${SPARK_HOME}/bin/spark-shell || notify_error_and_exit "Execution failed for shell"
else
    JOB_OUTPUT="${JOB_CONTROL_DIR}/output.log"
    tail -F "${JOB_OUTPUT}" &
    sudo -E java ${SPARK_JAVA_OPTS} -Djava.io.tmpdir=/mnt/hadoop $job_opts -cp ${JAR_PATH}:$(${SPARK_HOME}/bin/compute-classpath.sh) "${MAIN_CLASS}" "${JOB_NAME}" --date "${JOB_DATE}" --tag "${JOB_TAG}" --user "${JOB_USER}" --master "${JOB_MASTER}" >& "${JOB_OUTPUT}" || notify_error_and_exit "Execution failed for job ${JOB_WITH_TAG}"
fi

touch "${JOB_CONTROL_DIR}/SUCCESS"
