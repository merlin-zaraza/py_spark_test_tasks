# builder step used to download and configure spark environment
ARG SPARK_VERSION=3.0.2
ARG HADOOP_VERSION=3.2

FROM openjdk:11.0.11-jre-slim-buster as builder

# Add Dependencies for PySpark
ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN apt-get update && apt-get install -y curl vim wget dos2unix software-properties-common \
                                         ssh net-tools ca-certificates iputils-ping\
                                         python3 python3-pip python3-numpy \
                                         python3-matplotlib python3-scipy \
                                         python3-pandas python3-simpy \
                                         openssh-server openssh-client

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

# Generate ssh key
RUN ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
# Start ssh daemon
RUN service ssh start

# Apache spark environment
FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_APPS=/opt/spark-apps/main \
SPARK_DATA=/opt/spark-data \
SPARK_TEST=/opt/spark-apps/test \
SPARK_LOG=/opt/spark-log \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master" \
PYSPARK_PYTHON=python3 \
SERVICE_BASH=/opt/bash/service \
PATH="$PATH:$SPARK_HOME/bin"


ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_APPS:$PYTHONPATH \
WEB_APP=$SPARK_APPS/web \
WEB_APP_SCRIPT=app.py


EXPOSE 22 8080 7077 6066


ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

RUN mkdir -p $SERVICE_BASH $SPARK_TEST $SPARK_LOG

COPY bash/.bashrc /root/.bashrc
COPY bash/log4j.properties /opt/spark/conf/log4j.properties
COPY bash/start-spark.sh /opt/bash/start-spark.sh
COPY bash/service/install_python_packages.sh /opt/bash/service/install_python_packages.sh
COPY bash/service/requirements.txt /opt/bash/service/requirements.txt
COPY src/main/web/start_app.sh /opt/spark-apps/main/web/start_app.sh

RUN find /opt/spark-* -type f -print0 | xargs -0 dos2unix
RUN dos2unix /root/.bashrc /

RUN ["/bin/bash", "/opt/bash/service/install_python_packages.sh" ]
CMD ["/bin/bash", "-c" , "/opt/spark-apps/main/web/start_app.sh ; /opt/bash/start-spark.sh" ]
