ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION

# Keep Airflow's pinned dependency set intact while adding the libraries
# needed to call the existing ingestion package. dbt runs in its own environment.
COPY --chown=airflow:root requirements.txt /requirements.txt

USER airflow
RUN PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
    && pip install --no-cache-dir \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
       -r /requirements.txt
