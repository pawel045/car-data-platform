FROM python:3.13-alpine

WORKDIR /app

ENV PROJECT_ID="still-worthy-453018-t6"
ENV DATASET_ID="used_car_data"
ENV TABLE_ID="otomoto_data"

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "./etl/main.py"]
