FROM apache/airflow:3.0.3-python3.12

ARG TARGETARCH

USER root

RUN <<EOF
apt update -y
apt-get install -y openjdk-17-jre-headless
apt-get clean
rm -rf /var/lib/apt/lists/*
EOF

USER airflow

RUN <<EOF
pip install --upgrade pip
pip install apache-airflow-providers-apache-spark==5.3.1 pyspark==3.5.5
EOF

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV PATH=$JAVA_HOME/bin:$PATH