FROM apache/airflow:2.10.5

COPY .docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root
COPY .docker/airflow/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN mkdir -p /opt/data/warehouse && chmod -R 777 /opt/data/warehouse
USER airflow

ENTRYPOINT ["/entrypoint.sh"]
