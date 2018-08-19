#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo 'NODE: ' $(hostname)
export SPARK_HOME=$node_spark
export JAVA_HOME=$node_java

# $SPARK_HOME/sbin/start-master.sh
# if [ ! -f $mstr_log ]; then
    lockfile -r 0 $mstr_lock
    if [ $? -eq 0 ]; then
        python3.6 ./hadoopconf/hadoop_prep.py $HADOOP_HOME/etc/hadoop/core-site.xml  $HADOOP_HOME/etc/hadoop/yarn-site.xml $(hostname)
        rm -r $(datanode)
        echo $(hostname) >> $HADOOP_HOME/etc/hadoop/slaves
        echo 'y' | $HADOOP_HOME/bin/hdfs namenode -format
        $HADOOP_HOME/sbin/start-dfs.sh
        $HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
        $HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
        $HADOOP_HOME/sbin/start-yarn.sh 
        # MASTER_URI=$(curl -s http://localhost:8080/json/ | jq -r ".url")
        # echo $MASTER_URI > $mstr_log
        # ssh $head_ip $SPARK_HOME/bin/spark-submit --master $MASTER_URI --executor-cores=${SLURM_CPUS_PER_TASK} --executor-memory=${SLURM_MEM_PER_NODE}M $spscript&
        $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --executor-cores=${SLURM_CPUS_PER_TASK} --executor-memory=${SLURM_MEM_PER_NODE}M $spscript&
    else
        rm -r /home/centos/hdfs/data/dataNode/*
        echo $(hostname) >> $HADOOP_HOME/etc/hadoop/slaves
        $HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
        $HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
        # while [ ! -f $mstr_log ]; do sleep 5; done
        # MASTER_URI=$(head -n 1 $mstr_log)
    fi
# else
   #  MASTER_URI=$(head -n 1 $mstr_log)
# fi

echo 'RUNNING MASTER: ' $MASTER_URI

# $SPARK_HOME/sbin/start-slave.sh $MASTER_URI

# added sleep because the node's status is "ALIVE" initially
# needs to be fixed
sleep 200
curl -s http://localhost:8080/json/
# if [[ $(curl -s http://localhost:8080/json/ | jq -r ".status") == "ALIVE" ]]; then
#    $SPARK_HOME/sbin/stop-slave.sh
#    $SPARK_HOME/sbin/stop-master.sh
#    echo 'TERMINATED' >> $mstr_log
# fi
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/stop-yarn.sh
rm -f $mstr_lock
echo end $(date +%s.%N) >> $mstr_bench
