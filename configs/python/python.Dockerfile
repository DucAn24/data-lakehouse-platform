FROM python:3.11-slim

WORKDIR /app

# Copy and install requirements
COPY fastapi/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Expose port for services that need it (dashboard API)
EXPOSE 5000

# Default command (can be overridden in docker-compose.yml)
CMD ["python", "app.py"]