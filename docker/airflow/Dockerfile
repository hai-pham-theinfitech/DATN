FROM apache/airflow:2.7.2-python3.10

USER root


RUN apt-get update && \
    apt-get install -y sudo && \
    if ! getent group airflow > /dev/null; then groupadd --system --gid 50000 airflow; fi && \
    if ! getent passwd airflow > /dev/null; then useradd --system --uid 50000 --gid 50000 -m airflow; fi && \
    usermod -aG root airflow && \
    echo 'airflow ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/airflow-sudoers && \
    chmod 0440 /etc/sudoers.d/airflow-sudoers && \
    chown -R airflow:airflow /home/airflow /opt/airflow || true && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt Java 11 Development Kit và wget trong MỘT bước.
# apt-get update đảm bảo danh sách gói mới nhất.
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH 

# Định nghĩa phiên bản Spark
ARG SPARK_VERSION=3.5.1
# Tải, giải nén và di chuyển Spark đến /opt/spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Thiết lập biến môi trường SPARK_HOME
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH 
# Thiết lập PYTHONPATH cho PySpark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH 

# --- Bắt đầu phần thêm vào để tải các JAR Hadoop AWS ---
# Đảm bảo đang ở quyền root để có thể ghi vào /opt/spark/jars
USER root 
RUN apt-get update && apt-get install -y jq

# Khai báo biến phiên bản cho dễ quản lý
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.11.1026 # Phiên bản AWS SDK bundle tương thích với Hadoop 3.3.x

# Tải xuống Hadoop AWS và AWS SDK bundle JARs và đặt vào thư mục jars của Spark
RUN echo "Downloading Hadoop AWS and AWS SDK bundle JARs..." && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/spark/jars/ \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar -P /opt/spark/jars/ \
    && echo "JARs downloaded to /opt/spark/jars/"
# --- Kết thúc phần thêm vào ---

# ⬇️ THÊM PHẦN NÀY ĐỂ FIX QUYỀN CHO CRAWLER FOLDER ⬇️
# Tạo folder crawler và set quyền
RUN mkdir -p /opt/airflow/crawler && \
    chown -R airflow:root /opt/airflow/crawler && \
    chmod -R 775 /opt/airflow/crawler
# ⬆️ KẾT THÚC PHẦN THÊM ⬆️

# Chuyển sang user 'airflow' trước khi cài đặt các gói Python
USER airflow
RUN pip install pyspark
RUN pip install scrapy
RUN pip install deltalake
RUN pip install delta-spark pyspark minio
RUN pip install vn_address_converter
RUN pip install vietnamadminunits
RUN pip install pendulum
RUN pip install openai==2.3.0

RUN pip install --no-cache-dir --default-timeout=120 minio pyspark==3.5.6 delta-spark==3.0.0

