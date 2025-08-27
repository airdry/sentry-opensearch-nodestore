### Running Tests

The project includes two types of tests: fast unit tests that mock external services, and slower integration tests that run against a real OpenSearch instance via Docker.

#### Running Unit Tests

These tests do not require any external services. They are fast and should be run frequently during development.

```bash
# Run all tests that are NOT marked as 'integration'
poetry run pytest -m "not integration"
```

#### Running Integration Tests

These tests validate the full functionality against a live OpenSearch database.

**Prerequisites:**
-   [Docker](https://www.docker.com/get-started) and Docker Compose must be installed.

**Workflow:**

1.  **Start the OpenSearch Container**  
    This command starts an OpenSearch instance in the background with the security plugin disabled for easy testing.
    ```bash
    docker-compose up -d
    ```
    *Note: The first time you run this, it will download the OpenSearch image. Please wait 30-60 seconds for the service to become fully available.*

2.  **Run the Integration Test Suite**  
    Set the `RUN_INTEGRATION_TESTS` environment variable and use `pytest` to run only the tests marked as `integration`.
    ```bash
    # For Linux/macOS
    export RUN_INTEGRATION_TESTS=true
    poetry run pytest -m integration -v

    # For Windows (Command Prompt)
    # set RUN_INTEGRATION_TESTS=true
    # poetry run pytest -m integration -v
    ```

3.  **Stop the OpenSearch Container**  
    When you are finished, shut down the container to free up system resources.
    ```bash
    docker-compose down
    ```

#### Running All Tests

To run the complete test suite (both unit and integration tests), ensure the Docker container is running and execute:

```bash
export RUN_INTEGRATION_TESTS=true
poetry run pytest
```