ARG WORKER_IMAGE
FROM ${WORKER_IMAGE}
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends nftables \
 && rm -rf /var/lib/apt/lists/*
COPY deploy/j4 /app/deploy/j4
ENV PYTHONPATH=/app/python:/app
USER 65532:65532
