FROM centos:7

RUN yum -y update \
    && yum -y install epel-release \
    && yum -y install https://centos7.iuscommunity.org/ius-release.rpm \
    && yum -y install \
        python36u \
        python36u-devel \
        python36u-pip \

ENTRYPOINT ["/bin/bash"]
