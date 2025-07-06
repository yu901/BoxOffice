#!/bin/bash

PROJECT_HOME=~/pjt/BoxOffice
export AIRFLOW_HOME=${PROJECT_HOME}/airflow

# 웹서버 종료
if [ -f ${AIRFLOW_HOME}/webserver.pid ]; then
    kill $(cat ${AIRFLOW_HOME}/webserver.pid)
    rm ${AIRFLOW_HOME}/webserver.pid
    echo "Webserver stopped"
fi

# 스케줄러 종료
if [ -f ${AIRFLOW_HOME}/scheduler.pid ]; then
    kill $(cat ${AIRFLOW_HOME}/scheduler.pid)
    rm ${AIRFLOW_HOME}/scheduler.pid
    echo "Scheduler stopped"
fi
