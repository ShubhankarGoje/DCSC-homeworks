FROM python:3.8

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies (in this case, pandas)
RUN pip install pandas

# Make the script executable
RUN chmod +x docker.py

# Define the command to run your script
CMD ["docker.py"]