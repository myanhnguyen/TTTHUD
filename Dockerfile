# # Use OpenJDK 8 as the base image
FROM openjdk:8-jdk

# Install necessary tools and packages
RUN apt-get clean && apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    wget \
    curl \
    vim \
    nano \
    postgresql \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

# Download and Install Spark
RUN set -eux; \
    wget -O /tmp/spark-3.2.0-bin-hadoop3.2.tgz https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz && \
    tar xvzf /tmp/spark-3.2.0-bin-hadoop3.2.tgz -C /opt/ && \
    mv /opt/spark-3.2.0-bin-hadoop3.2 /opt/spark && \
    rm /tmp/spark-3.2.0-bin-hadoop3.2.tgz

# Set SPARK_HOME environment variable
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Upgrade pip and setuptools, and install PySpark and psycopg2-binary
RUN pip3 install --upgrade pip setuptools
RUN pip3 install pyspark psycopg2-binary

# Copy entrypoint script, CSV file, and Python script
COPY entrypoint.sh /usr/local/bin/
COPY data.csv /data/data.csv
COPY spark_sql.py /spark_sql.py

# Make the entrypoint script executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# Expose ports
EXPOSE 8080 4040 7077 5432

# Start the container with the entrypoint script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
