# Simple Python Docker App

This application demonstrates a complete ETL (Extract, Transform, Load) workflow within a Docker container:
1.  **Extract**: Connects to a PostgreSQL database and runs a query.
2.  **Transform**: Saves the query results into a local CSV file.
3.  **Load**: Uploads the generated CSV file to an S3-compatible bucket using `s3cmd`.

The application is controlled by the `APP_ENV` environment variable (`dev`, `qa`, `prod`), which determines which `.env.{APP_ENV}` file to use for configuration.

## Configuration
Credentials and endpoints for PostgreSQL and S3 are stored in:
- `.env.dev`
- `.env.qa`
- `.env.prod`

**IMPORTANT**: These files contain sensitive credentials. Ensure they are populated correctly and **never commit them to public version control**.

### Production Security Note
For production environments, it is strongly recommended to use a dedicated secrets management service (like AWS Secrets Manager, HashiCorp Vault, or Docker Secrets) instead of `.env` files. Secrets should be securely injected into the container at runtime.

## How to Run

1.  **Prerequisites**:
    * Docker is installed.
    * You have a running PostgreSQL database and an S3 bucket accessible with the credentials you provide.
    * The `.env.*` files in the project root are filled with your credentials.

2.  **Build the Docker image:**
    ```sh
    docker build -t simple-docker-app .
    ```

3.  **Run the ETL process for a specific environment:**
    Use the `-e` flag to set the `APP_ENV` variable.

    * **Development:**
        ```sh
        docker run --rm -e APP_ENV=dev simple-docker-app
        ```

    * **QA:**
        ```sh
        docker run --rm -e APP_ENV=qa simple-docker-app
        ```

    * **Production:**
        ```sh
        docker run --rm -e APP_ENV=prod simple-docker-app
        ```