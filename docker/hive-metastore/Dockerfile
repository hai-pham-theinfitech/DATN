FROM ubuntu:22.04

# Cài Java, công cụ tiện ích
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    wget \
    curl \
    netcat-traditional \
    procps \
    vim \
    postgresql-client \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Tạo user hive
RUN useradd -ms /bin/bash hive

# Cài Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz -P /tmp/ && \
    tar -xzf /tmp/hadoop-3.3.6.tar.gz -C /opt/ && \
    mv /opt/hadoop-3.3.6 /opt/hadoop && \
    rm /tmp/hadoop-3.3.6.tar.gz && \
    mkdir -p /opt/hadoop/tmp && chmod -R 777 /opt/hadoop/tmp

# Cài Hive
RUN wget https://archive.apache.org/dist/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz -P /tmp/ && \
    tar -xzf /tmp/apache-hive-3.1.2-bin.tar.gz -C /opt/ && \
    mv /opt/apache-hive-3.1.2-bin /opt/hive && \
    rm /tmp/apache-hive-3.1.2-bin.tar.gz

ENV HIVE_HOME=/opt/hive
ENV PATH=$HIVE_HOME/bin:$PATH

# Copy các JAR hỗ trợ S3A sau khi Hive đã cài
COPY docker/hive-metastore/hadoop-lib/*.jar /opt/hive/lib/

# Copy JDBC PostgreSQL driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.20.jar -P /opt/hive/lib/

# Cài TEZ (nếu cần)
RUN wget https://archive.apache.org/dist/tez/0.10.2/apache-tez-0.10.2-bin.tar.gz -P /tmp/ && \
    tar -xzf /tmp/apache-tez-0.10.2-bin.tar.gz -C /opt/ && \
    cp /opt/apache-tez-0.10.2-bin/*.jar /opt/hive/lib/ && \
    cp /opt/apache-tez-0.10.2-bin/lib/*.jar /opt/hive/lib/ && \
    rm -rf /opt/apache-tez-0.10.2-bin /tmp/apache-tez-0.10.2-bin.tar.gz

# Tạo folder config Hive
RUN mkdir -p /opt/hive/conf

# Copy config hive-site.xml + script chạy metastore
COPY docker/hive-metastore/hive-site.xml /opt/hive/conf/
COPY docker/hive-metastore/start_metastore.sh /usr/local/bin/

# Fix permission + định dạng dòng
RUN dos2unix /usr/local/bin/start_metastore.sh && chmod +x /usr/local/bin/start_metastore.sh

# Tạo thư mục warehouse
RUN mkdir -p /user/hive/warehouse && chown -R hive:hive /user/hive/warehouse && chown -R hive:hive /opt/hive

USER hive

# Khởi động metastore
ENTRYPOINT ["/usr/local/bin/start_metastore.sh"]
