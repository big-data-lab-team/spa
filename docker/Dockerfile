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
    && yum -y install wget

RUN wget -qO- http://mirror.dsrg.utoronto.ca/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz | tar xzv

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/
ENV SPARK_HOME /spark-2.3.1-bin-hadoop2.7
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin 

COPY spark-defaults.conf $SPARK_HOME/conf/
COPY spark-env.sh $SPARK_HOME/conf

RUN mkdir -pv /mnt/vhs/spark-logs

WORKDIR /mnt/vhs



ENTRYPOINT ["/bin/bash"]
