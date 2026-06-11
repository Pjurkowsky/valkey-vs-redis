FROM python:3.12-slim

RUN pip install --no-cache-dir "redis[hiredis]"

WORKDIR /work
COPY consistency_check.py /work/consistency_check.py
COPY split_brain_check.py /work/split_brain_check.py
COPY failover_client.py /work/failover_client.py

ENTRYPOINT ["python", "/work/consistency_check.py"]
