FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# the platform client library (build context "dataplatform" in docker-compose.system.yml)
COPY --from=dataplatform . /opt/dataplatform
RUN pip install --no-cache-dir /opt/dataplatform

COPY *.py ./
COPY sinks/ ./sinks/
# the data contracts, for the contracts job (python -m dataplatform.contracts apply contracts)
COPY --from=contracts . ./contracts/

CMD ["python", "-u", "writer.py", "sinks/cdr_events.yaml"]
