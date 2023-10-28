FROM python:3.11-slim

WORKDIR /app

COPY input_data.xlsx .
COPY requirements.txt .
COPY *.py .
COPY ./tests /app/tests/

RUN pip install -r requirements.txt

CMD ["python3", "main.py"]
