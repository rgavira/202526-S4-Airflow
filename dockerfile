FROM apache/hadoop:3.4.1

USER root

# Fix CentOS repos
RUN mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak 2>/dev/null || true

RUN cat <<EOF > /etc/yum.repos.d/CentOS-Base.repo
[base]
name=CentOS-7 - Base
baseurl=http://vault.centos.org/centos/7/os/x86_64/
enabled=1
gpgcheck=0

[updates]
name=CentOS-7 - Updates
baseurl=http://vault.centos.org/centos/7/updates/x86_64/
enabled=1
gpgcheck=0

[extras]
name=CentOS-7 - Extras
baseurl=http://vault.centos.org/centos/7/extras/x86_64/
enabled=1
gpgcheck=0
EOF

RUN yum clean all && yum makecache

# Install required packages
RUN yum install -y \
    java-11-openjdk-devel \
    wget \
    curl \
    nc \
    dos2unix \
    && yum clean all

ENV JAVA_HOME=/usr/lib/jvm/jre
ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/hive
ENV PATH=${HIVE_HOME}/bin:${PATH}

# Download and install Hive
RUN wget -q https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    mv apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

# Download MySQL connector for Hive Metastore
RUN wget -q https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.30.tar.gz && \
    tar -xzf mysql-connector-java-8.0.30.tar.gz && \
    cp mysql-connector-java-8.0.30/mysql-connector-java-8.0.30.jar ${HIVE_HOME}/lib/ && \
    rm -rf mysql-connector-java-8.0.30*

# Create scripts directory
RUN mkdir -p /scripts

# Copy and fix scripts
COPY scripts/ /scripts/
RUN find /scripts -type f -name "*.sh" -exec dos2unix {} \; 2>/dev/null || true && \
    chmod +x /scripts/*.sh 2>/dev/null || true

# Expose Hive ports
EXPOSE 9083 10000 10002

CMD ["/bin/bash"]