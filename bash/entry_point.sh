#!/bin/bash

set -o pipefail
set -e

echo "start ssh"
/usr/sbin/sshd -D &

if ! /opt/spark-apps/main/web/start_app.sh ; then
  echo "Cannot start web app"
  exit 1
fi

if ! /opt/bash/start-spark.sh ; then
  echo "Cannot start spark"
  exit 1
fi

