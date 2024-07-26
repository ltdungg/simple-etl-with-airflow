#!/usr/bin/env bash
airflow resetdb
airflow db init
airflow db upgrade
airflow users create -r Admin -u admin -e admin@admin.com -f admin -l admin -p admin
airflow scheduler &
airflow webserver