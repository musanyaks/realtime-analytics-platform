FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies for R and ODBC
RUN apt-get update && apt-get install -y \
    r-base \
    r-base-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    unixodbc-dev \
    libbz2-dev \
    liblzma-dev \
    libpcre2-dev \
    libblas-dev \
    liblapack-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Install R packages needed for pipeline
RUN R -e "install.packages(c(\
    'DBI', \
    'odbc', \
    'dplyr', \
    'jsonlite', \
    'lubridate', \
    'logger', \
    'config', \
    'reticulate' \
    ), repos='https://cloud.r-project.org/')"

# Install dbt and Snowflake adapter
RUN pip install --no-cache-dir \
    dbt-core==1.7.0 \
    dbt-snowflake==1.7.0 \
    apache-airflow-providers-snowflake \
    pyarrow \
    pandas \
    snowflake-connector-python

USER airflow
