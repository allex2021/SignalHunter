FROM python:3.11-slim

# Install system dependencies for pyttsx3 TTS
# espeak/espeak-ng is required for Linux TTS; libespeak-dev for compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    espeak \
    espeak-ng \
    libespeak1 \
    libespeak-ng1 \
    libespeak-dev \
    libespeak-ng-dev \
    ffmpeg \
    alsa-utils \
    pulseaudio \
    pulseaudio-utils \
    gcc \
    libasound2 \
    libasound2-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set PulseAudio to run in daemon-less mode for container environments
ENV PULSE_LATENCY_MSEC=30

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Environment defaults
ENV REDIS_URL=redis://redis:6379/0
ENV SIGNAL_STREAM_KEY=stream:signals
ENV AUDIO_ALERT_THRESHOLD=7.0
ENV TTS_RATE=165
ENV TTS_VOLUME=0.9
ENV BLOCK_TIMEOUT_MS=5000
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Non-root user (audio group for sound access)
RUN useradd -m -u 1001 -G audio worker 2>/dev/null || useradd -m -u 1001 worker
RUN chown -R worker:worker /app

USER worker

# Health check — verifies Redis connection
HEALTHCHECK --interval=60s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import redis; r = redis.Redis.from_url('$REDIS_URL'); r.ping()" || exit 1

CMD ["python", "-u", "worker.py"]
