#!/bin/bash

# For first time run use L_IN_BUILD_IMAGE = "y" it will build spark cluster image
#    example ./bash/start-docker.sh y
# To start project with tests run set L_IN_RUN_TEST="y"
#    example ./bash/start-docker.sh n y
# To rerun failed tests set L_IN_RUN_TEST="f"
#    example ./bash/start-docker.sh n f

set -eEu
set -o pipefail

declare -l L_IN_BUILD_IMAGE=${1:-n}
declare -l L_IN_RUN_TEST=${2:-n}
declare -l L_IN_SPARK_VERSION=${3:-3.0.2}
declare -l L_IN_HADOOP_VERSION=${4:-3.2}
declare -l L_IN_IMAGE_NAME=${5:-cluster-apache-spark}

declare -l l_spark_master_container

export DOCKER_SPARK_IMAGE=$L_IN_IMAGE_NAME:$L_IN_SPARK_VERSION
export SPARK_VERSION=$L_IN_SPARK_VERSION
export HADOOP_VERSION=$L_IN_HADOOP_VERSION

function fn_run_command() {
  declare l_in_command=${1:?}
  declare l_in_err_msg=${2:?}
  declare l_in_err_code=${3:-1}
  declare -l l_in_exit=${4:-y}

  declare l_proc_exit_code=0

  echo "************************************************************"
  echo "Running $l_in_command"
  echo "************************************************************"

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

  fn_run_command "l_spark_master_container=\$( docker compose ps | grep '\-spark\-master\-1' | cut -d ' ' -f1 )" \
                 "Cannot get docker project name"\
                 "30"\
                 "n"
}

if [[ "$L_IN_BUILD_IMAGE" == 'y' ]]; then

  rm -f ~/.docker/config.json
#  sudo apt-get update
#  sudo apt-get install dos2unix
#
#  dos2unix ./*

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

l_test_cmd=""

if  [[ "$L_IN_RUN_TEST" != "n" ]]; then

  # rerun failed
  l_rerun_flag=""
  if [[ "$L_IN_RUN_TEST" == "f" ]]; then
    l_rerun_flag=" --lf"
  fi

  l_test_cmd="-c 'pytest /opt/spark-test $l_rerun_flag'"
fi

fn_run_command "docker container exec -it ${l_spark_master_container} /bin/bash $l_test_cmd"  \
               "Cannot connect to the master"\
               "50"