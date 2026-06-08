FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and keyword config
COPY . .

# Environment defaults
ENV REDIS_URL=redis://redis:6379/0
ENV REDDIT_CLIENT_ID=""
ENV REDDIT_CLIENT_SECRET=""
ENV REDDIT_USER_AGENT="signal-aggregator-miner/1.0"
ENV KEYWORD_SPIKE_THRESHOLD=300
ENV ANALYSIS_WINDOW_MINUTES=120
ENV TICK_INTERVAL_SECONDS=120
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Non-root user
RUN useradd -m -u 1001 miner && chown -R miner:miner /app
USER miner

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import redis; r = redis.Redis.from_url('$REDIS_URL'); r.ping()" || exit 1

CMD ["python", "-u", "miner.py"]
