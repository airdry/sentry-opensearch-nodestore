# Test Guide

## Unit tests

- Fast and offline.
- No real OpenSearch required (tests use fakes).
- Sentry dependency is stubbed in `tests/conftest.py` so you don’t need to install the full Sentry package.

## Integration tests

- Disabled by default.
- Use the Docker Compose setup in `docker-compose.yml`:
  - HTTP (no auth) on `localhost:9200`
  - HTTPS (requires auth) on `localhost:9201`
- Verify connectivity and the backend’s basic read/write behavior against real OpenSearch.

---

## Run unit tests (default)

```sh
poetry run pytest -q
```

## Run integration tests

```sh
RUN_INTEGRATION_TESTS=1 poetry run pytest -q
```