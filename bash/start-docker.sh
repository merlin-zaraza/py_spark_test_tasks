#!/bin/bash

set -eEu
set -o pipefail

typeset -l L_IN_BUILD_IMAGE=${1:-n}
typeset -l L_IN_SPARK_VERSION=${2:-3.0.2}
typeset -l L_IN_HADOOP_VERSION=${3:-3.2}
typeset -l L_IN_IMAGE_NAME=${4:-cluster-apache-spark}

typeset -l l_proj_folder

export DOCKER_SPARK_IMAGE=$L_IN_IMAGE_NAME:$L_IN_SPARK_VERSION
export SPARK_VERSION=$L_IN_SPARK_VERSION
export HADOOP_VERSION=$L_IN_HADOOP_VERSION

function fn_run_command() {
  typeset l_in_command=${1:?}
  typeset l_in_err_msg=${2:?}
  typeset l_in_err_code=${3:-1}
  typeset -l l_in_exit=${4:-y}

  typeset l_proc_exit_code=0

  echo "Running $l_in_command"

  if ! eval $l_in_command ; then
    echo "$l_in_err_msg"
    l_proc_exit_code=1

    if  [[ "$l_in_exit" == "y" ]]; then
      exit $l_in_err_code
    fi

  fi

  return $l_proc_exit_code
}

function fn_get_proj_folder() {

  fn_run_command "l_proj_folder=\$( docker compose ls | egrep -v "^NAME" | tail -1 | cut -d ' ' -f1 )" \
                 "Cannot get docker project name"\
                 "30"\
                 "n"
}

if [[ "$L_IN_BUILD_IMAGE" == 'y' ]]; then

  fn_run_command "docker compose down" \
                 "Cannot Stop project"\
                 "10"\
                 "n"

  fn_run_command "docker build --build-arg SPARK_VERSION=$L_IN_SPARK_VERSION --build-arg HADOOP_VERSION=$L_IN_HADOOP_VERSION -t $DOCKER_SPARK_IMAGE ./ "\
                 "Cannot build image"\
                 "20"
fi

if ! fn_get_proj_folder ; then

  fn_run_command "docker compose up -d" \
                 "Cannot Start project"\
                 "40"

  if ! fn_get_proj_folder ; then
    exit 45
  fi

fi

fn_run_command "docker container exec -it ${l_proj_folder}-spark-master-1 /bin/bash" \
               "Cannot connect to the master"\
               "50"