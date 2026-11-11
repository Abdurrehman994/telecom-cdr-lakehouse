FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Write print() output to the container logs immediately instead of buffering it
ENV PYTHONUNBUFFERED=1

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# the platform client library and the contract of the decisions this service publishes
# (build contexts "dataplatform" and "contracts" in docker-compose.system.yml)
COPY --from=dataplatform . /opt/dataplatform
RUN pip install --no-cache-dir /opt/dataplatform
COPY --from=contracts fraud-decisions-value.avsc contracts/

# Copy the service code
COPY ml_service.py .

# Expose the port
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8001/health')"

# Run the service
CMD ["uvicorn", "ml_service:app", "--host", "0.0.0.0", "--port", "8001"]