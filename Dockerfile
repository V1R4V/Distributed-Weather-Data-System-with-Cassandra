FROM ubuntu:22.04
RUN apt-get update; apt-get install -y wget curl openjdk-17-jdk python3-pip iproute2

# Python stuff
RUN pip3 install numpy==2.1.3 pyspark==3.4.1 cassandra-driver==3.28.0 grpcio==1.58.0 grpcio-tools==1.58.0

# Install packages in requirements.txt, you can add more packages you need to the requirements.txt file
COPY /src/requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

# SPARK
RUN wget https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz && \
    tar -xf spark-3.5.7-bin-hadoop3.tgz && \
    rm spark-3.5.7-bin-hadoop3.tgz

# CASSANDRA
RUN wget https://dlcdn.apache.org/cassandra/5.0.6/apache-cassandra-5.0.6-bin.tar.gz && \
    tar -xf apache-cassandra-5.0.6-bin.tar.gz && \
    rm apache-cassandra-5.0.6-bin.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${PATH}:/apache-cassandra-5.0.6/bin:/spark-3.4.1-bin-hadoop3.2/bin"

COPY cassandra.sh /cassandra.sh
CMD ["sh", "/cassandra.sh"]