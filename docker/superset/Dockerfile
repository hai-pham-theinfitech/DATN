# docker/superset/Dockerfile
FROM apache/superset:437

USER root

# Cài đặt các gói hệ thống cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện Python cần thiết
RUN pip install --upgrade pip && \
    pip install \
    trino \
    pandas \
    pyarrow \
    requests

USER superset