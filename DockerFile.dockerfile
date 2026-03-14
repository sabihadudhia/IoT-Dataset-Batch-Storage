FROM python:3.12.6
WORKDIR /app
COPY requirements.txt .
COPY python_main.py .
COPY iot_telemetry_data.csv .
RUN pip install -r requirements.txt
