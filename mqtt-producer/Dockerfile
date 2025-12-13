FROM python:3.11-slim

WORKDIR /app
COPY producer.py .

RUN pip install paho-mqtt

CMD ["python", "producer.py"]
