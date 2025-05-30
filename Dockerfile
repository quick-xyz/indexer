FROM python:3.11-slim

WORKDIR /app

# Copy package and requirements
COPY indexer/ ./indexer/
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy configs (can be overridden with volume mounts)
COPY config/ ./config/

# Copy deployment script
COPY scripts/deploy.py .

# Create runtime directories
RUN mkdir -p data logs

# Set environment variable for config location
ENV CONFIG_PATH=/app/config/config.json

CMD ["python", "deploy.py"]