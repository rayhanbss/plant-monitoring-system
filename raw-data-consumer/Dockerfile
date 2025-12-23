FROM python:3.11-slim

WORKDIR /app

RUN pip install confluent-kafka
RUN pip install cassandra-driver

COPY . .

CMD ["python", "consumer.py"]