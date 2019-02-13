#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo 'NODE: ' $(hostname) $(hostname -i)

module load spark/2.3.0

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM_FLOAT=$(echo "${SLURM_MEM_PER_NODE} * 0.95" | bc)
export SLURM_SPARK_MEM=${SLURM_SPARK_MEM_FLOAT%.*}

echo $SLURM_SPARK_MEM

$SPARK_HOME/sbin/start-master.sh
if [ ! -f $mstr_log ]; then
    lockfile -r 0 $mstr_lock
    if [ $? -eq 0 ]; then
	while [ -z "$MASTER_URI" ]
	do
		MASTER_URI=$(grep -Po '(?=spark://).*' $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master*.out)
		sleep 5
	done
        echo $MASTER_URI > $mstr_log
    else
        while [ ! -f $mstr_log ]; do sleep 5; done
        MASTER_URI=$(head -n 1 $mstr_log)
    fi
else
    MASTER_URI=$(head -n 1 $mstr_log)
fi

while [[ -z "$MASTER_UI_PORT" ]]
do
    echo "loading logfile data"
    MASTER_UI_PORT=$(grep -Po "'MasterUI' on port.*" $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master*.out | awk '{print $NF}')
    MASTER_UI_PORT=${MASTER_UI_PORT/./}
    REST_SERVER_PORT=$(grep -Po "Started REST server.*" $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master*.out | awk '{print $NF}')
    MASTER_ALIVE=$(grep 'ALIVE' $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.apache.spark.deploy.master*.out)
done
echo $MASTER_ALIVE
sleep 60

echo 'RUNNING MASTER: ' $MASTER_URI
echo 'Master UI port: ' $MASTER_UI_PORT
echo 'REST server port: ' $REST_SERVER_PORT

WORKER_OUT=$($SPARK_HOME/sbin/start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} $MASTER_URI 2>&1)
WORKER_LOG=$(echo $WORKER_OUT | grep "starting org.apache.spark.deploy.worker.Worker" | awk '{print $NF}')
WORKER_UI=$(grep "WorkerWebUI" $WORKER_LOG | awk '{print $NF}')/json/
echo $WORKER_OUT
echo 'Worker log file' $WORKER_LOG
echo 'Worker UI url: ' $WORKER_UI

if [ ! -z "$driver_prog" ]; then
    MASTER_URI=${MASTER_URI/%????/$REST_SERVER_PORT} # cluster mode requires REST port
    echo 'Cluster deploy master: ' $MASTER_URI

    eval $driver_prog > output 2>&1
    cat output
    driverid=`cat output | grep submissionId | grep -Po 'driver-\d+-\d+'`
    echo 'Spark driver ID: ' $driverid

    DRIVER_REST=${MASTER_URI/spark/http}
    DRIVER_REST=$DRIVER_REST/v1/submissions/status/$driverid
    echo 'REST API Url: ' $DRIVER_REST
    curl $DRIVER_REST
fi

cores_in_use='$([[ $(curl -s $WORKER_UI | jq -r ".coresused") == "0" ]] && { echo false; } || { echo true; })'
executors_complete='$([[ $(curl -s $WORKER_UI | jq -r ".finishedexecutor" | jq 'if length = 0 then "true" else "false" end') == "true" ]] && { echo false; } || { echo true; })'

idle_count=0
elapsed_time=0
start_time="$(date -u +%s)"

while true; do
    echo 'executor running'
    MASTER_UI=${MASTER_URI/spark/http}
    MASTER_UI=${MASTER_UI/%????/$MASTER_UI_PORT}
    curl -s $WORKER_UI
    echo $idle_count
    [[ $(eval $cores_in_use) == "false" ]] && idle_count=$((idle_count + 1)) || idle_count=0

    if [ $idle_count -ge 0 ]; then
        end_time="$(date -u +%s)"
	elapsed_time="$(($end_time-$start_time))"
	sleep 1
    else
	sleep 5
    fi

    [ $elapsed_time -ge 120 ] && break

done
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
rm -f $mstr_lock
echo end $(date +%s.%N) >> $mstr_bench
