FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY db_writer.py .

CMD ["python", "-u", "db_writer.py"]
