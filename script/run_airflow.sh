#!/bin/bash

PROJECT_HOME=~/pjt/BoxOffice
export AIRFLOW_HOME=${PROJECT_HOME}/airflow
export AIRFLOW__CORE__DAGS_FOLDER=${PROJECT_HOME}/dags
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True


mkdir -p ${AIRFLOW_HOME}/logs
airflow db migrate
airflow api-server --port 8081 > ${AIRFLOW_HOME}/logs/webserver.log 2>&1 &

echo $! > ${AIRFLOW_HOME}/webserver.pid
echo "Webserver started on port 8081"

airflow scheduler > ${AIRFLOW_HOME}/logs/scheduler.log 2>&1 &

echo $! > ${AIRFLOW_HOME}/scheduler.pid
echo "Scheduler started"
