FROM python:3.12-slim

RUN pip install --no-cache-dir "redis[hiredis]"

WORKDIR /work
COPY backup_restore_seed.py /work/backup_restore_seed.py

ENTRYPOINT ["python", "/work/backup_restore_seed.py"]
