FROM apache/superset:v5.0.0

USER root

RUN <<EOF
apt-get update -y
apt-get install g++ -y
EOF

COPY --chmod=777 superset_config.py /app/
COPY --chmod=777 requirements-local.txt /app/docker/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

ENTRYPOINT ["/app/docker/docker-bootstrap.sh", "app-gunicorn"]