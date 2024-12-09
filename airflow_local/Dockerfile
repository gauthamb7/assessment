# Use the base image
FROM apache/airflow:2.10.3

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements file into the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
