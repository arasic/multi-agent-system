FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Non-root runtime user (invariant I-7/I-11; antipatterns A8). Workers never need root.
RUN useradd --create-home --uid 1000 --shell /usr/sbin/nologin mas \
 && mkdir -p /data /app && chown -R mas:mas /data /app

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY mas ./mas
# ".[llm]" = the vendor SDK too: the gateway service (the one process with a vendor key) builds real providers from
# this same image; workers/orchestrator never hold a key and never import it.
RUN pip install --no-cache-dir ".[llm]"

COPY benchmarks ./benchmarks
COPY acceptance ./acceptance
RUN chown -R mas:mas /app

ENV PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    MAS_REPO_ROOT=/data/repos \
    MAS_WORKTREE_ROOT=/data/worktrees \
    GIT_CONFIG_NOSYSTEM=1

USER mas
VOLUME ["/data"]
CMD ["mas", "--help"]
