# Review simulator (spec 4.3, ADR 008). Same pinned base image as the
# generator; dependencies installed from a hashed lock file.
FROM python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

COPY simulator.py .

RUN useradd --system --no-create-home reviewer
USER reviewer

ENTRYPOINT ["python", "simulator.py"]
