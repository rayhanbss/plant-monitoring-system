FROM python:3.11-slim

WORKDIR /app

COPY Requirements.txt .

RUN pip install -r Requirements.txt

COPY . .

CMD ["streamlit", "run", "app.py"]