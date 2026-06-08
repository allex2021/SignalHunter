FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (Chromium only for minimal image size)
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code
COPY . .

# Environment defaults (override via docker-compose or env file)
ENV REDIS_URL=redis://redis:6379/0
ENV SOCIAL_WINDOW_MINUTES=120
ENV SWEEP_SLEEP_SECONDS=300
ENV MAX_CONCURRENT_PAGES=5
ENV PROXY_LIST_PATH=""
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Non-root user for security
RUN useradd -m -u 1001 scraper && chown -R scraper:scraper /app
USER scraper

# Health check — verifies Redis connectivity
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import redis; r = redis.Redis.from_url('$REDIS_URL'); r.ping()" || exit 1

CMD ["python", "-u", "scraper.py"]
