#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
export SPARK_HOME=$mstr_spark
export JAVA_HOME=$mstr_java
MASTER_URI=$(grep -Po 'spark://.*' $($SPARK_HOME/sbin/start-master.sh | grep -Po '/.*out'))
echo $MASTER_URI > $mstr_log
srun -n 1 -N 1 $SPARK_HOME/bin/spark-submit --master $MASTER_URI --executor-cores ${SLURM_CPUS_PER_TASK} --executor-memory ${SLURM_MEM_PER_NODE}M $spscript
$SPARK_HOME/sbin/stop-master.sh
echo 'TERMINATED' >> $mstr_log
echo end $(date +%s.%N) >> $mstr_bench
