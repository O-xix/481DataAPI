FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install minimal build tools in case some packages need compilation (pandas often installs wheels,
# but some environments may need the build toolchain). Keep layers small by cleaning apt cache.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY flask_csv_api ./flask_csv_api

# Expose the default Flask port
EXPOSE 5000

# Use gunicorn as the production server. The module path is `flask_csv_api.app:app`.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "flask_csv_api.app:app", "--workers", "2", "--threads", "4"]
