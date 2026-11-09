FROM python:3.12-slim

WORKDIR /app
# Pinned client version; the mock has no other dependencies.
RUN pip install --no-cache-dir confluent-kafka==2.6.1
COPY ledger.py .
USER 65534
ENTRYPOINT ["python", "-u", "ledger.py"]
