#!/bin/bash

#SBATCH --account=def-glatard
#SBATCH --time=00:05:00
#SBATCH --nodes=5
#SBATCH --mem=20G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=1

logdir=/scratch/vhayots/sparkpilot/logs
mstr_bench=$logdir/hpc_def_${SLURM_JOB_ID}_benchmarks.out
spscript="/scratch/vhayots/sparkpilot/example/dummyprogram.py /scratch/vhayots/sparkpilot/dummy-1.out -p 12 -c /scratch/vhayots/sparkpilot/checkpoints"

echo start $(date +%s.%N) > $mstr_bench

module load spark/2.3.0
#module load python

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM=$(printf "%.0f" $((${SLURM_MEM_PER_NODE} *0.95)))

start-master.sh
while [ -z "$MASTER_URL" ]
do
	MASTER_URL=$(curl -s http://localhost:8080/json/ | jq -r ".url")
	echo "master not found"
	sleep 5
done

NWORKERS=$((SLURM_NTASKS - 1))

SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NWORKERS} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
slaves_pid=$!

srun -n 1 -N 1 spark-submit --master=${MASTER_URL} --executor-memory=${SLURM_SPARK_MEM}M $spscript

kill $slaves_pid
stop-master.sh

echo end $(date +%s.%N) >> $mstr_bench
