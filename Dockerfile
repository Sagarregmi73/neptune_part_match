# Use an official Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all app code
COPY . .

# Copy local .env (optional, only for local dev)
# For EC2, we'll inject env via docker run
COPY env/local.env .env

# Expose port
EXPOSE 8000

# Default command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
