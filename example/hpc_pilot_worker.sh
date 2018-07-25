#!/bin/bash

hostname > $wrkr_log
echo start $(date +%s.%N) > $wrkr_log

module load spark
module load python

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR

while [ ! -f $mstr_log ]; do sleep 10; done
MASTER_URL=$(head -n 1 $mstr_log)
echo $MASTER_URL

start_slave.sh $MASTER_URL

WORKER_PID=$(ls /tmp | grep -Po 'org.apache.spark.deploy.worker.Worker-\d+.pid' | grep -Po '\d+')

while [[ $(tail -n 1 $mstr_log) != "TERMINATED" ]]; do sleep 5; done

stop-slave.sh
spark-daemon.sh status org.apache.spark.deploy.worker.Worker $WORKER_PID
echo end $(date +%s.%N) >> $wrkr_log
