FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install minimal build tools in case some packages need compilation (pandas often installs wheels,
# but some environments may need the build toolchain). Keep layers small by cleaning apt cache.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential unzip \
 && rm -rf /var/lib/apt/lists/*

# Default PORT used by the Flask app when not overridden. Aligns with `app.py` default (8080).
ENV PORT=8080

# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- Additions for Downloading Data ---
# This section downloads the dataset during the image build.

# 1. Create a directory for Kaggle credentials.
RUN mkdir -p /root/.kaggle/

# 2. Use a build secret to securely provide the kaggle.json file from Google Secret Manager.
#    The secret 'kaggle_credentials' is defined in cloudbuild.yaml.
RUN --mount=type=secret,id=kaggle_credentials,target=/root/.kaggle/kaggle.json \
    # 3. Download the dataset from Kaggle.
    kaggle datasets download -d sobhanmoosavi/us-accidents -p /app/flask_csv_api --unzip
# --- End of Additions ---

# Copy application source
COPY flask_csv_api ./flask_csv_api

# Expose the port the app listens on (informational). The app respects $PORT.
EXPOSE 8080

# Use gunicorn as the production server. Bind to the PORT env var so the container honors
# the same default as `app.py` and can be overridden at runtime with -e PORT=... or Docker
# orchestration platforms.
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT} flask_csv_api.app:app --workers 2 --threads 4"]
