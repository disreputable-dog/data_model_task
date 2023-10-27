FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
COPY main.py .
COPY input_data.xlsx .

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
