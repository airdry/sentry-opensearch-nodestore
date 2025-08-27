# Sentry OpenSearch NodeStore

[![License](https://img.shields.io/pypi/l/sentry-opensearch-nodestore.svg)](https://github.com/your-username/sentry-opensearch-nodestore/blob/main/LICENSE)

An asynchronous Sentry [NodeStorage](https://develop.sentry.dev/services/nodestore/) backend designed for **OpenSearch**.

This package provides a Sentry-compatible NodeStorage implementation that uses the modern asynchronous client `opensearch-py` to store and retrieve event data. It is intended to be used within a Sentry environment as a custom backend plugin.

## Features

-   **Asynchronous:** Built with `asyncio` and `opensearch-py`'s `AsyncOpenSearch` client for high-performance, non-blocking I/O.
-   **Highly Configurable:** Index settings like shards, replicas, codec, and index patterns can be configured via environment variables.
-   **Automatic Template Management:** On startup, it creates a fully configured index template to ensure new daily indices have the correct mappings and settings.
-   **Time-Based Cleanup:** Includes a `cleanup` method to automatically delete old indices based on a configurable cutoff date.
-   **Robust and Tested:** Comes with a full suite of unit and integration tests to ensure reliability.

## Installation

```bash
pip install sentry-opensearch-nodestore
```

## Usage

Here is a basic example of how to instantiate and use the NodeStore. This code would typically be integrated into your Sentry configuration.

```python
import asyncio
from datetime import datetime, timezone, timedelta
from opensearchpy import AsyncOpenSearch
from sentry_opensearch_nodestore import AsyncOpenSearchNodeStorage

async def main():
    # 1. Initialize the async OpenSearch client
    #    For a real application, configure hosts, SSL, and auth as needed.
    os_client = AsyncOpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}]
    )

    # 2. Instantiate the NodeStore
    nodestore = AsyncOpenSearchNodeStorage(os_client=os_client)

    # 3. Bootstrap the backend (creates the index template if it doesn't exist)
    print("--- Running Bootstrap ---")
    await nodestore.bootstrap()
    print("Bootstrap complete.")

    # 4. Use the nodestore methods
    node_id = "event_abc123"
    node_data = b'{"message": "hello from opensearch"}'

    await nodestore._set_bytes(node_id, node_data)
    retrieved_data = await nodestore._get_bytes(node_id)
    
    print(f"Retrieved: {retrieved_data.decode('utf-8')}")

    await nodestore.delete(node_id)
    print(f"Deleted node: {node_id}")

    # 5. Run cleanup for indices older than 10 days
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=10)
    await nodestore.cleanup(cutoff_date)
    print("Cleanup complete.")

    # 6. Close the client connection when your application shuts down
    await os_client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

The backend is configured via environment variables, allowing for flexible deployment without code changes.

| Environment Variable                               | Description                                                                                              | Default Value |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ------------- |
| `SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS`     | The number of primary shards for new indices created by the template.                                    | `3`           |
| `SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS`   | The number of replica shards for new indices created by the template.                                    | `1`           |
| `SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN`        | The index pattern that the template will apply to.                                                       | `sentry-*`    |
| `SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC`          | The compression codec used for storing data. `zstd` is a good balance. `best_compression` uses more CPU for smaller size. | `zstd`        |

## Development and Testing

This project uses [Poetry](https://python-poetry.org/) for dependency management and testing.

### 1. Initial Setup

First, clone the repository and install the development dependencies using Poetry.

```bash
git clone https://github.com/your-username/sentry-opensearch-nodestore.git
cd sentry-opensearch-nodestore
poetry install
```

### 2. Running Tests

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

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
