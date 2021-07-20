FROM python:3.9-slim
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache-dir
COPY . /ingest-price
