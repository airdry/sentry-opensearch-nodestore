# sentry-opensearch-nodestore

Sentry NodeStore backend powered by OpenSearch.

Supported Sentry 24.x / 25.x and OpenSearch 2.x / 3.x.

Use an OpenSearch cluster to store Sentry NodeStore payloads for better scalability and simpler retention (delete old indices) compared to PostgreSQL.

- Stores payloads as compressed, non-indexed data
- Daily indices with optional prefix (e.g., `sentry-YYYY-MM-DD` or `sentry-<prefix>-YYYY-MM-DD`)
- Reads/deletes via alias (default: `sentry`)
- Composable index templates (`/_index_template`)

---

## Why OpenSearch for NodeStore

- Horizontal scalability by adding data nodes
- Sharding and replication for throughput and resilience
- Automatic rebalancing when cluster grows
- Cleanup is fast and reliable by dropping old daily indices (vs. large PostgreSQL tables)

---

## Installation

Install the package for your Sentry deployment.

### Option A: From PyPI

```sh
pip install sentry-opensearch-nodestore
```

### Option B: Rebuild Sentry image with this backend

```dockerfile
FROM getsentry/sentry:25.8.0
RUN pip install sentry-opensearch-nodestore

```
---
## Environment variables
| Variable | Default | Required | Notes |
|---|---:|:---:|---|
| `SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS` | `3` | No | Number of primary shards per daily index. Must be an integer. |
| `SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA` | `1` | No | Number of replicas per daily index. Must be an integer. |
| `SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN` | `sentry-*` | No | Must be a single value (one pattern). Accepts a plain string, a JSON array with exactly one item, or a comma-separated list that resolves to exactly one item. Used for the composable index template. |
| `SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC` | `zstd` | No | Index codec. Use `best_compression` if your cluster doesn’t support `zstd`. |
| `SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX` | — | No | Optional prefix for index names. If set (e.g., `dev`), indices become `sentry-<prefix>-YYYY-MM-DD` (e.g., `sentry-dev-2025-08-29`). If not set, indices are `sentry-YYYY-MM-DD`. |



## Configuration

Set the Sentry NodeStore backend and provide an OpenSearch client. The backend reads its settings from uppercase environment variables (see “Environment variables” below).

### Option A: Use method [self-hosted](https://github.com/getsentry/self-hosted/blob/master/sentry/sentry.conf.example.py)

```python
# sentry.conf.py
import os
from opensearchpy import OpenSearch

# Option 1 (preferred): set these in your process environment (Docker/Compose, systemd, shell)
# Option 2: set here before Sentry constructs the NodeStore:
os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX"] = "dev"          # optional
os.environ["SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS"] = "1"        # default: 3
os.environ["SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA"] = "0"       # default: 1
os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN"] = "sentry-dev-*"  # single value
# Optional (default: zstd). Use best_compression for broad compatibility across cluster versions:
os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC"] = "best_compression"

os_client = OpenSearch(
    ["https://admin:myStrongPassword123!@opensearch:9200"],
    http_compress=True,
    verify_certs=False,   # demo TLS only; in production, verify with a real CA/fingerprint
    timeout=60,
    ssl_show_warn=False,
    # For production TLS, prefer certificate verification or fingerprint pinning:
    # ssl_assert_fingerprint="AA:BB:CC:...:ZZ"
)

SENTRY_NODESTORE = "sentry_opensearch_nodestore.backend.OpenSearchNodeStorage"
SENTRY_NODESTORE_OPTIONS = {
    "es": os_client,
    # Optional overrides:
    # "alias_name": "sentry",
    # "template_name": "sentry",
    # "index": "sentry-{prefix}-{date}",  # default; resolved by env INDEX_PREFIX -> "sentry-<prefix>-{date}" or "sentry-{date}"
    # "refresh": False,  # default; set True only if you require read-after-write in-line
}

# Keep Sentry defaults
from sentry.conf.server import *  # noqa

# Ensure the app is importable (if needed for Django discovery)
INSTALLED_APPS = list(INSTALLED_APPS)
INSTALLED_APPS.append("sentry_opensearch_nodestore")
INSTALLED_APPS = tuple(INSTALLED_APPS)

```

### Option B: Use helm chart [sentry] (https://github.com/sentry-kubernetes/charts/tree/develop/charts/sentry)

```yaml
  sentryConfPy: |
    # No Python Extension Config Given
    from opensearchpy import OpenSearch
    os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX"] = "dev"
    os.environ["SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA"] = "0"
    os.environ["SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS"] = "1"
    os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN"] = "sentry-dev-*"
    # Optional (default is zstd):
    os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC"] = "best_compression"
    os_client = OpenSearch(
          ['https://admin:myStrongPassword123!@10.129.0.36:9200'],
          http_compress=True,
          verify_certs=False,
          timeout=60,
          ssl_show_warn=False  
      )
    SENTRY_NODESTORE = 'sentry_opensearch_nodestore.OpenSearchNodeStorage'
    SENTRY_NODESTORE_OPTIONS = {
        'es': os_client,
        'refresh': False
    }

    INSTALLED_APPS = list(INSTALLED_APPS)
    INSTALLED_APPS.append('sentry_opensearch_nodestore')
    INSTALLED_APPS = tuple(INSTALLED_APPS)
```