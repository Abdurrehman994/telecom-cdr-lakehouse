FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY writer.py query.py maintenance.py ./

CMD ["python", "-u", "writer.py"]
