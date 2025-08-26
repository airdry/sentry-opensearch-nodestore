# Sentry OpenSearch NodeStore

An asynchronous Sentry [NodeStorage](https://develop.sentry.dev/services/nodestore/) backend for OpenSearch.

This package provides a Sentry-compatible NodeStorage implementation that uses an asynchronous OpenSearch client (`opensearch-py`) to store and retrieve event data.

## Installation

```bash
pip install sentry-opensearch-nodestore
```

## Configuration

The backend is configured via environment variables.

-   `SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS`: The number of primary shards for new indices. Defaults to `3`.
-   `SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS`: The number of replica shards for new indices. Defaults to `1`.
-   `SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN`: The index pattern for the template. Defaults to `"sentry-*"`.
-   `SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC`: The compression codec for the index. Defaults to `"zstd"`. Other options include `best_compression` or `default`.

## Usage

Here is a basic example of how to instantiate and use the NodeStore.

```python
import asyncio
from opensearchpy import AsyncOpenSearch
from sentry_opensearch_nodestore import AsyncOpenSearchNodeStorage

async def main():
    # 1. Initialize the async OpenSearch client
    os_client = AsyncOpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}]
    )

    # 2. Instantiate the NodeStore
    nodestore = AsyncOpenSearchNodeStorage(os_client=os_client)

    # 3. Bootstrap the backend (creates the index template)
    await nodestore.bootstrap()

    # 4. Use the nodestore methods
    node_id = "event_abc123"
    node_data = b'{"message": "hello from opensearch"}'

    await nodestore.set(node_id, node_data)
    retrieved_data = await nodestore.get(node_id)
    print(f"Retrieved: {retrieved_data}")

    await nodestore.delete(node_id)

    # 5. Close the client connection
    await os_client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Development

To set up the development environment, clone the repository and run:

```bash
poetry install
```
