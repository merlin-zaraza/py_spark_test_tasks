#!/bin/bash

l_pid=$(pidof -x python3 $WEB_APP_SCRIPT)

if [[ -z "$l_pid" ]]; then
    echo "App is not running"
else
    echo "Killing $l_pid"
    kill -9 $l_pid
fi