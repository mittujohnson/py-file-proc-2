# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Set a default environment for the application
ENV APP_ENV dev

# Copy the requirements file into the container
COPY requirements.txt ./

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the .env files into the image
# These provide the configuration for each environment
COPY .env.dev .env.dev
COPY .env.qa .env.qa
COPY .env.prod .env.prod

# Copy the rest of the application's code to the container
COPY ./app ./app

# Specify the command to run on container start
ENTRYPOINT ["python", "-m", "app.main"]