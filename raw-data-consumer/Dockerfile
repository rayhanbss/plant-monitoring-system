FROM python:3.11-slim

WORKDIR /app
COPY consumer.py .
COPY schema.cql .

RUN pip install kafka-python
RUN pip install cassandra-driver

CMD ["python", "consumer.py"]