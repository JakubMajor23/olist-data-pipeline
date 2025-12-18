# Use the official Airflow image as a base
FROM apache/airflow:2.10.2

# Switch to root to install system dependencies (optional, but git is useful for dbt)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user to install Python packages
USER airflow

# Copy our requirements file to the container
COPY requirements.txt /requirements.txt

# Install libraries
RUN pip install --no-cache-dir -r /requirements.txt