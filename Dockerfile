# builder step used to download and configure spark environment
ARG SPARK_VERSION=3.0.2
ARG HADOOP_VERSION=3.2

FROM openjdk:11.0.11-jre-slim-buster as builder

# Add Dependencies for PySpark
ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN apt-get update && apt-get install -y curl vim wget software-properties-common \
                                         ssh net-tools ca-certificates \
                                         python3 python3-pip python3-numpy \
                                         python3-matplotlib python3-scipy \
                                         python3-pandas python3-simpy

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive
ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


# Apache spark environment
FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master" \
PYSPARK_PYTHON=/usr/bin/python\
PATH="$PATH:$PYSPARK_PYTHON"\
PATH="$PATH:/opt/spark/bin"


EXPOSE 8080 7077 6066


ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY bash/start-spark.sh /
COPY bash/.bashrc /root/.bashrc
COPY bash/log4j.properties /opt/spark/conf/log4j.properties

CMD ["/bin/bash", "/start-spark.sh"]