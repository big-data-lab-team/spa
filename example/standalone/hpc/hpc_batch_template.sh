#!/bin/bash

spscript="--jars /home/vhayots/projects/def-glatard/vhayots/niftijio/target/scala-2.12/*.jar /home/vhayots/projects/def-glatard/vhayots/spa/example/scala_increment/target/scala-2.11/increment-app_2.11-1.0.jar /scratch/vhayots/splits /scratch/vhayots/scalaout 1"

echo start $(date +%s.%N) > $mstr_bench

module load spark/2.3.0

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=$SLURM_TMPDIR
export SLURM_SPARK_MEM_FLOAT=$(echo "${SLURM_MEM_PER_NODE} * 0.95" | bc)
export SLURM_SPARK_MEM=${SLURM_SPARK_MEM_FLOAT%.*}

start-master.sh
while [ -z "$MASTER_URL" ]
do
	MASTER_URL=$(curl -s http://localhost:8080/json/ | jq -r ".url")
	echo "master not found"
	sleep 5
done

NWORKERS=$((SLURM_NTASKS))

SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS} -N ${NWORKERS} --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &
slaves_pid=$!

if [ ! -z "$driver_prog" ]
then 
    srun -n 1 -N 1 $driver_prog
fi

kill $slaves_pid
stop-master.sh

echo end $(date +%s.%N) >> $mstr_bench
