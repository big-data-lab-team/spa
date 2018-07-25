#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench

module load spark
module load python

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR

MASTER_URL=$(grep -Po 'spark://.*' $(start-master.sh | grep -Po '/.*out'))
echo $MASTER_URL > $mstr_log

srun -n 1 -N 1 spark-submit --master=$MASTER_URL --executor-cores=$SLURM_CPUS_PER_TASK --executor-memory=${SLURM_MEM_PER_NODE}M $spscript

stop-master.sh

echo 'TERMINATED' >> $mstr_log
echo end $(date +%s.%N) >> $mstr_bench

