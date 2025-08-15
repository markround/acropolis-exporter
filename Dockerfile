FROM python:3.12-slim
LABEL maintainer="ymark.dastmalchiround@nutanix.com"
LABEL description="NKP Cluster Cleaner - Delete CAPI clusters based on label criteria"
LABEL version="0.1.0"

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

RUN groupadd -r nutanix && useradd -r -g nutanix -s /bin/bash nutanix
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY exporter.py .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Switch to non-root user
USER nutanix

# Set the entrypoint to the nkp-cluster-cleaner command
ENTRYPOINT ["/app/exporter.py"]
