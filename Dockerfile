FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    iputils-ping \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
    
#copy ETL code
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY etl.py .
#command to run ETL
CMD ["python", "etl.py"]