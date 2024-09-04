# Base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code into the container
COPY . .

# Default command
CMD ["python", "main.py", "--config", "config.json"]
