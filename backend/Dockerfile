# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the local requirements file (backend/requirements.txt) into the container at /app
# This assumes the Docker build context is the 'backend' directory.
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir to reduce image size
# Using --default-timeout to prevent timeouts on slow networks if any package is large
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --default-timeout=100 -r requirements.txt

# Copy the rest of the backend application code into the container at /app
# This will copy everything from the backend directory (build context)
COPY . .

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable (optional, if you want to control log level, etc.)
# ENV LOG_LEVEL=info

# Run uvicorn server when the container launches
# Use 0.0.0.0 to ensure it's accessible from outside the container
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
