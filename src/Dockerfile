FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
  ca-certificates curl gpg lsb-release && \
  curl -fsSL https://packages.redis.io/gpg | \
  gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg && \
  echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] \
  https://packages.redis.io/deb $(lsb_release -cs) main" \
  > /etc/apt/sources.list.d/redis.list && \
  apt-get update && \
  apt-get install -y memtier-benchmark valkey && \
  rm -rf /var/lib/apt/lists/*

# install kubectl
RUN curl -L -o /usr/local/bin/kubectl \
  https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl && \
  chmod +x /usr/local/bin/kubectl

WORKDIR /work
COPY benchmark.sh /work/run.sh
RUN chmod +x /work/run.sh


# ENTRYPOINT ["/work/run.sh"]