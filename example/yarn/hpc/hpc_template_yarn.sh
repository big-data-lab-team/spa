#!/bin/bash

echo start $(date +%s.%N) > $mstr_bench
echo 'NODE: ' $(hostname)

module load spark/2.3.0
module load python/3.7.0

export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

lockfile -r 0 $mstr_lock
if [ $? -eq 0 ]; then
    	python ./hadoopconf/hadoop_prep.py $HADOOP_HOME/etc/hadoop/core-site.xml  $HADOOP_HOME/etc/hadoop/yarn-site.xml $(hostname)
	python ./hadoopconf/hdfs_prep.py $HADOOP_HOME/etc/hadoop/hdfs-site.xml $(echo $SLURM_TMPDIR)
    	rm -r $datanode
    	echo $(hostname) >> $HADOOP_HOME/etc/hadoop/slaves
    	echo 'y' | $HADOOP_HOME/bin/hdfs namenode -format
    	$HADOOP_HOME/sbin/start-dfs.sh
    	$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
    	$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
   	$HADOOP_HOME/sbin/start-yarn.sh
    	$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --executor-cores=${SLURM_CPUS_PER_TASK} --executor-memory=${SLURM_MEM_PER_NODE}M $spscript&
else
	rm -r $datanode
	echo $(hostname) >> $HADOOP_HOME/etc/hadoop/slaves
	$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
	$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager
fi

while [ -z "$appId" ]
do
	appId=$(grep -rw $logdir -e "Submitted application" | grep -oe 'application_[0-9]\{13\}_[0-9]\{4\}')
	sleep 5
done

while [[ $stat != "SUCCEEDED" && $stat != "FAILED" ]]
do
	stat=$(yarn application -status $appId | grep -e 'Final-State' | grep -E -o "SUCCEEDED|FAILED")
done

echo $stat

$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/stop-yarn.sh
rm -f $mstr_lock
echo end $(date +%s.%N) >> $mstr_bench
