# Serving API (ADR 018). Same pinned base image as the simulators;
# dependencies installed from a hashed lock file.
FROM python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

COPY app.py .

RUN useradd --system --no-create-home api
USER api

EXPOSE 8000
# One process: the endpoints are plain (sync) functions, which FastAPI runs
# in a thread pool (40 threads), plenty for a demo. More throughput would
# mean more uvicorn workers, each with its own Redis connection pool.
ENTRYPOINT ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
