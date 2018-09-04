FROM centos:7


RUN yum -y update \
    && yum -y install epel-release \
    && yum -y install https://centos7.iuscommunity.org/ius-release.rpm \
    && yum -y install \
        python36u \
        python36u-devel \
        python36u-pip \
    && yum -y install java-1.8.0-openjdk \
    && yum -y install procmail \
    && yum -y install jq \
    && yum -y install wget \
    && yum -y install which \
    && yum -y install openssh-server openssh-clients

RUN wget -qO- http://mirror.dsrg.utoronto.ca/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz | tar xzv
# RUN wget -qO- http://mirror.dsrg.utoronto.ca/apache/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz | tar xzv

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/
ENV SPARK_HOME /spark-2.3.1-bin-hadoop2.7
# ENV HADOOP_HOME /hadoop-2.7.7
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin 

COPY spark-defaults.conf $SPARK_HOME/conf/
COPY spark-env.sh $SPARK_HOME/conf

RUN source $SPARK_HOME/conf/spark-env.sh

EXPOSE 8080

RUN pip3.6 install slurmpy pyspark pytest pytest-cov
# EXPOSE 22

# RUN ["/usr/bin/ssh-keygen", "-A"]

# RUN ["/usr/sbin/sshd", "-D"]

# RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa \
#    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys \
#    && chmod 0600 /root/.ssh/authorized_keys

RUN mkdir -pv /mnt/vhs/spark-logs

WORKDIR /mnt/vhs

ENTRYPOINT ["/bin/bash"]
