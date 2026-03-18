FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "run.py", \
     "--input", "data.csv", \
     "--config", "config.yaml", \
     "--output", "metrics.json", \
     "--log-file", "run.log"]
