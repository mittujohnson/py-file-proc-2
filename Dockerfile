# Use a full Debian-based Python image to get access to apt
FROM python:3.9

# Set the working directory in the container
WORKDIR /usr/src/app

# Set a default environment for the application
ENV APP_ENV=dev

# Install system dependencies:
# - libpq-dev is required for the psycopg2 Python package
# - s3cmd is the command-line tool for S3 uploads
RUN apt-get update && apt-get install -y \
    libpq-dev \
    s3cmd \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt ./

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the .env files into the image
COPY .env.dev .env.dev
COPY .env.qa .env.qa
COPY .env.prod .env.prod

# Copy the rest of the application's code to the container
COPY ./app ./app

# Specify the command to run on container start.
# Note: The original request had a string parameter. This workflow doesn't need it,
# but you could add it back to pass in a table name or query.
ENTRYPOINT ["python", "-m", "app.main"]