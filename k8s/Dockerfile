FROM redislabs/memtier_benchmark:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -L -o /usr/local/bin/kubectl \
      https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl && \
    chmod +x /usr/local/bin/kubectl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /work
COPY benchmark.sh /work/run.sh
RUN chmod +x /work/run.sh
