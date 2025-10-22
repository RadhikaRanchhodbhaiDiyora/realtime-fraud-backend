# Base Spark image (Using the generic 'latest' tag for high reliability)
FROM jupyter/pyspark-notebook:latest 

# Set environment variables for Redis
ENV REDIS_HOST=redis

# Install Python dependencies needed for the script (Redis client)
# We switch to root user to ensure global installation permissions
USER root
RUN pip install redis

# Set working directory (scripts will be mapped here)
WORKDIR /app

# Switch back to the default user ('jovyan') for container execution
USER jovyan