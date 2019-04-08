#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo $mstr_bench > singlenodes.out

module load spark/2.3.0

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM_FLOAT=$(echo "${SLURM_MEM_PER_NODE} * 0.95" | bc)
export SLURM_SPARK_MEM=${SLURM_SPARK_MEM_FLOAT%.*}
echo ${SLURM_SPARK_MEM}

term_handler()
{
    stop-master.sh
    echo end $(date +%s.%N) >> $mstr_bench

	exit -1
}
trap 'term_handler' TERM

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

if [ ! -z "$program" ]
then 
    srun -n 1 -N 1 spark-submit --master ${MASTER_URL} --executor-memory ${SLURM_SPARK_MEM}M $program
fi

kill $slaves_pid
stop-master.sh

echo end $(date +%s.%N) >> $mstr_bench
