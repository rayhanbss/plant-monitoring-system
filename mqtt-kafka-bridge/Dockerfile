FROM python:3.11-slim

WORKDIR /app
COPY bridge.py .

RUN pip install kafka-python
RUN pip install paho-mqtt

CMD ["python", "bridge.py"]