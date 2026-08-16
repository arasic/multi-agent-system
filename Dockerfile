FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY mas ./mas
RUN pip install --no-cache-dir .

COPY benchmarks ./benchmarks
COPY acceptance ./acceptance

ENV PYTHONUNBUFFERED=1
CMD ["mas", "--help"]
