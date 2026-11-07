# One small process. The database is NOT in this image.
# Fly mounts a volume at /data (see fly.toml) and the app stores SQLite there.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py storage.py ./

# Non-secret path. The directory is the volume mount, created by Fly.
ENV DOME_RELAY_DB=/data/dome-relay.db

EXPOSE 8080

# --factory calls app_factory(), which refuses to boot if a secret is missing.
# One worker only. Two workers would fight over the same SQLite file.
CMD ["uvicorn", "app:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
