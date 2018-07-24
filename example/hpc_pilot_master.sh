#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench

module load spark
module load python

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR

MASTER_URL=$(grep -Po 'spark://.*' $($SPARK_HOME/sbin/start-master.sh | grep -Po '/.*out'))

NWORKERS=$((SLURM_NTASKS - 1))

SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NWORKERS} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_MEM_PER_NODE}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
slaves_pid=$!

srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --executor-memory ${SLURM_MEM_PER_NODE}M $spscript

kill $slaves_pid
stop-master.sh

echo end $(date +%s.%N) >> $mstr_bench
