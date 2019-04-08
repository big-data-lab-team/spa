#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo $mstr_bench > singlenodes.out

module load spark/2.3.0

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM_FLOAT=$(echo "${SLURM_MEM_PER_NODE} * 0.95" | bc)
export SLURM_SPARK_MEM=${SLURM_SPARK_MEM_FLOAT%.*}

term_handler()
{
    echo end $(date +%s.%N) >> $mstr_bench

	exit -1
}
trap 'term_handler' TERM
    
srun -n 1 -N 1 spark-submit --master local[${SLURM_CPUS_PER_TASK}] --driver-memory ${SLURM_SPARK_MEM}M $program

echo end $(date +%s.%N) >> $mstr_bench
