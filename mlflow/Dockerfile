FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir mlflow==2.14.1 "boto3>=1.34.0"

WORKDIR /mlflow

COPY entrypoint.sh /mlflow/entrypoint.sh
RUN chmod +x /mlflow/entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/mlflow/entrypoint.sh"]
