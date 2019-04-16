#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo $mstr_bench > singlenodes.out

module load spark/2.3.0

export SPARK_IDENT_STRING=$SLURM_JOBID
export SPARK_WORKER_DIR=sworker_logs
#$SLURM_TMPDIR
SLURM_MEM_PER_PROC=$(echo "${SLURM_NPROCS} / ${SLURM_NNODES}" | bc)
export SLURM_MEM_PER_TASK=$(echo "${SLURM_MEM_PER_NODE} / ${SLURM_MEM_PER_PROC}" | bc)
export SLURM_SPARK_MEM_FLOAT=$(echo "${SLURM_MEM_PER_TASK} * 0.95" | bc)
export SLURM_SPARK_MEM=${SLURM_SPARK_MEM_FLOAT%.*}

term_handler()
{
    kill $slaves_pid
    stop-master.sh
    echo end $(date +%s.%N) >> $mstr_bench

    exit -1
}
trap 'term_handler' TERM

batch_program()
{
    start-master.sh
    while [ -z "$MASTER_URL" ]
    do
	    MASTER_URL=$(curl -s http://localhost:8080/json/ | jq -r ".url")
	    echo "master not found"
	    sleep 5
    done

    NWORKERS_TOTAL=$((SLURM_NTASKS))
    NWORKERS=$(echo "${NWORKERS_TOTAL} / ${SLURM_NNODES}" | bc)
    NWORKERS=${NWORKERS%.*}

    echo 'Number of workers per node' $NWORKERS
    echo 'Number of nodes' $SLURM_NNODES
    echo 'Memory per task' $SLURM_SPARK_MEM

    NNODES_REMOTE=$((SLURM_NNODES - 1))

    SPARK_WORKER_INSTANCES=${NWORKERS} SPARK_NO_DAEMONIZE=1 srun -n ${NWORKERS_TOTAL} -N $SLURM_NNODES --label --output=$SPARK_LOG_DIR/spark-%j-workers.out start-slave.sh -m ${SLURM_SPARK_MEM}M -c ${SLURM_CPUS_PER_TASK} ${MASTER_URL} &

    slaves_pid=$!

    if [ ! -z "$program" ]
    then 
        spark-submit --master ${MASTER_URL} --executor-memory ${SLURM_SPARK_MEM}M $program
    fi

    kill $slaves_pid
    stop-master.sh

    echo end $(date +%s.%N) >> $mstr_bench
}

batch_program &
wait $!
