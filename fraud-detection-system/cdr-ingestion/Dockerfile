FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# the platform client library and this service's data contract
# (build contexts "dataplatform" and "contracts" in docker-compose.system.yml)
COPY --from=dataplatform . /opt/dataplatform
RUN pip install --no-cache-dir /opt/dataplatform
COPY --from=contracts cdr-events-value.avsc contracts/

COPY . .

EXPOSE 8080

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
