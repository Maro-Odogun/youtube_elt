ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Copy requirements as airflow user
COPY --chown=airflow:root requirements.txt /

# Install additional packages (remove the redundant apache-airflow installation)
RUN pip install --no-cache-dir -r /requirements.txt