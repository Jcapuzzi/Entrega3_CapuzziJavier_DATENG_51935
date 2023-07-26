FROM apache/airflow:2.6.2

RUN python -m pip install --upgrade pip
RUN python -m pip install apache-airflow-providers-apache-spark

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
RUN python -m pip install --upgrade pip
RUN python -m pip install yfinance
RUN python -m pip install SQLAlchemy
RUN python -m pip install pyarrow
RUN python -m pip install psycopg2-binary
