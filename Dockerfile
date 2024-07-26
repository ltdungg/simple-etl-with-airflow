FROM apache/airflow:latest
USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags
COPY requirements.txt .

USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

USER ${AIRFLOW_UID}