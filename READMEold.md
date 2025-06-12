# Simple Python Docker App

This is a simple Python application that runs in a Docker container. It demonstrates how to manage configurations for different environments (`dev`, `qa`, `prod`) using `.env` files.

The application is controlled by the `APP_ENV` environment variable. Based on this variable, it loads settings from the corresponding `.env.{APP_ENV}` file and prints them.

## Configuration
Configuration variables are stored in:
- `.env.dev`
- `.env.qa`
- `.env.prod`

Each file contains environment-specific settings like `LOG_LEVEL` and `API_ENDPOINT`.

## How to Run

1.  **Build the Docker image:**
    ```sh
    docker build -t simple-docker-app .
    ```

2.  **Run the container for a specific environment:**
    Use the `-e` flag to set the `APP_ENV` variable.

    * **Development (default):**
        ```sh
        docker run --rm simple-docker-app "Running dev tests..."
        ```

    * **QA:**
        ```sh
        docker run --rm -e APP_ENV=qa simple-docker-app "Executing QA validation..."
        ```

    * **Production:**
        ```sh
        docker run --rm -e APP_ENV=prod simple-docker-app "Processing production data."
        ```