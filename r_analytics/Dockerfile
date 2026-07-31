FROM rocker/shiny-verse:4.3.2

# Install system dependencies
RUN apt-get update && apt-get install -y \
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

# Install R packages
RUN install2.r --error --skipinstalled \
    shiny \
    shinydashboard \
    plotly \
    DT \
    dplyr \
    dbplyr \
    DBI \
    odbc \
    sparklyr \
    kafka \
    jsonlite \
    lubridate \
    rmarkdown \
    knitr \
    kableExtra \
    ggplot2 \
    tidyr \
    scales \
    config \
    logger \
    testthat \
    lintr

# Copy app
COPY . /app
WORKDIR /app

EXPOSE 3838

CMD ["R", "-e", "shiny::runApp('app.R', host='0.0.0.0', port=3838)"]