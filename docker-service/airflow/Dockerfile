FROM apache/airflow:2.8.1-python3.10

WORKDIR /opt/airflow

USER root

# Install OpenJDK 17 (You can change this to install a different version of Java)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow    

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# TODO(Allen): Change the JAVA_HOME for AMD64

COPY ./python_requirements.txt ./

RUN pip3 install -r python_requirements.txt

COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

# ENTRYPOINT [ "/bin/bash" ]