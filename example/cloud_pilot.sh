#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo 'NODE: ' $(hostname)
export SPARK_HOME=$node_spark
export JAVA_HOME=$node_java

$SPARK_HOME/sbin/start-master.sh
if [ ! -f $mstr_log ]; then
    lockfile -r 0 $mstr_lock
    if [ $? -eq 0 ]; then
        MASTER_URI=$(curl -s http://localhost:8080/json/ | jq -r ".url")
        echo $MASTER_URI > $mstr_log
        ssh $head_ip $SPARK_HOME/bin/spark-submit --master $MASTER_URI --executor-cores=${SLURM_CPUS_PER_TASK} --executor-memory=${SLURM_MEM_PER_NODE}M $spscript&
        rm -f $mstr_lock
    else
        while [ ! -f $mstr_log ]; do sleep 5; done
        MASTER_URI=$(head -n 1 $mstr_log)
    fi
else
    MASTER_URI=$(head -n 1 $mstr_log)
fi

echo 'RUNNING MASTER: ' $MASTER_URI

$SPARK_HOME/sbin/start-slave.sh $MASTER_URI

# added sleep because the node's status is "ALIVE" initially
# needs to be fixed
sleep 200
if [[ $(curl -s http://localhost:8080/json/ | jq -r ".status") == "ALIVE" ]]; then
    $SPARK_HOME/sbin/stop-slave.sh
    $SPARK_HOME/sbin/stop-master.sh
    echo 'TERMINATED' >> $mstr_log
fi
echo end $(date +%s.%N) >> $mstr_bench
