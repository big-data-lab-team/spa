#!/bin/bash
#SBATCH --time=$wrkr_time
#SBATCH --nodes=$wrkr_nodes
#SBATCH --mem=$wrkr_mem
#SBATCH --cpus-per-task=$wrkr_cpus
#SBATCH --ntasks-per-node=$wrkr_tasks


hostname > $wrkr_log
echo start $(date +%s.%N) >> $wrkr_log
export SPARK_HOME=$wrkr_spark
export JAVA_HOME=$wrkr_java
while [ ! -f $mstr_log ]; do sleep 10; done
MASTER_URI=$(head -n 1 $mstr_log)
echo $MASTER_URI
$SPARK_HOME/sbin/start-slave.sh $MASTER_URI
WORKER_PID=$(ls /tmp | grep -Po 'org.apache.spark.deploy.worker.Worker-\d+.pid' | grep -Po '\d+')
while [[ $(tail -n 1 $mstr_log) != "TERMINATED" ]]; do sleep 5; done
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/spark-daemon.sh status org.apache.spark.deploy.worker.Worker $WORKER_PID
echo end $(date +%s.%N) >> $wrkr_log
