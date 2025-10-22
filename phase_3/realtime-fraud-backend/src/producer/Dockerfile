# Start from a standard Python image
FROM python:3.9-slim
# Set working directory
WORKDIR /app
# Install dependencies from the requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy the producer script (actual code copy is done via volume in docker-compose)