#!/bin/bash

if [ "$SPARK_WORKLOAD" == "master" ]; then

  l_log=$SPARK_LOG/web_app.log

  if $WEB_APP/stop_app.sh 2>&1 | tee $l_log ; then
    echo "Starting app" | tee -a $l_log
    python3 $WEB_APP/app.py 2>&1 | tee -a $l_log &

    sleep 1

    if l_pid=$(pidof -x python3 $WEB_APP_SCRIPT) ; then
        echo "App started. PID : $l_pid" | tee -a $l_log
    fi
  fi
fi