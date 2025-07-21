FROM apache/airflow:2.8.1-python3.9

USER root
RUN pip install --no-cache-dir -r requirements.txt
COPY dag.py /opt/airflow/dags/dag.py

USER  airflow