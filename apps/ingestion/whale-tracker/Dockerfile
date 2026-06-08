FROM python:3.11-slim

WORKDIR /app

# Install build dependencies for web3 / lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libssl-dev \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1001 csa

# Install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY tracker.py ./
COPY known-wallets.json ./

# Switch to non-root user
USER csa

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python -c "import asyncio; asyncio.run(asyncio.sleep(0)); print('ok')" || exit 1

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "-u", "tracker.py"]
