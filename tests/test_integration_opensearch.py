# tests/test_integration_opensearch.py
import os
import time
from datetime import datetime, timezone

import pytest
from opensearchpy import OpenSearch, exceptions

from sentry_opensearch_nodestore.backend import OpenSearchNodeStorage


HTTP_HOST = {"host": "localhost", "port": 9200, "scheme": "http"}
HTTPS_HOST = {"host": "localhost", "port": 9201, "scheme": "https"}
ADMIN_AUTH = ("admin", "myStrongPassword123!")


def wait_for_cluster(client: OpenSearch, timeout=60):
    start = time.time()
    last_exc = None
    while time.time() - start < timeout:
        try:
            if client.ping():
                return
        except Exception as e:
            last_exc = e
        time.sleep(1)
    raise TimeoutError(f"OpenSearch cluster ping did not succeed within {timeout}s. Last error: {last_exc!r}")


def make_http_client():
    return OpenSearch(
        hosts=[HTTP_HOST],
        timeout=30,
    )


def make_https_client(with_auth: bool):
    kwargs = dict(
        hosts=[HTTPS_HOST],
        timeout=30,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    if with_auth:
        kwargs["http_auth"] = ADMIN_AUTH
    return OpenSearch(**kwargs)


@pytest.mark.integration
def test_http_connection_no_auth():
    client = make_http_client()
    try:
        wait_for_cluster(client, timeout=90)
    except TimeoutError as e:
        pytest.skip(f"HTTP cluster not ready: {e}")

    assert client.ping() is True
    info = client.info()
    assert "version" in info


@pytest.mark.integration
def test_https_connection_requires_auth_and_allows_with_auth():
    authed_client = make_https_client(with_auth=True)
    try:
        wait_for_cluster(authed_client, timeout=120)
    except TimeoutError as e:
        pytest.skip(f"HTTPS cluster not ready: {e}")

    noauth_client = make_https_client(with_auth=False)
    failed_as_expected = False
    try:
        ok = noauth_client.ping()
        if not ok:
            failed_as_expected = True
    except Exception:
        failed_as_expected = True

    assert failed_as_expected, "HTTPS cluster should require auth; ping without auth must fail"

    assert authed_client.ping() is True
    info = authed_client.info()
    assert "version" in info


def _run_backend_roundtrip(es_client: OpenSearch, alias: str, env_prefix: str):
    os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX"] = env_prefix
    os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN"] = f"sentry-{env_prefix}-*"
    os.environ["SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS"] = "1"
    os.environ["SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA"] = "0"
    os.environ["SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC"] = "best_compression"

    try:
        storage = OpenSearchNodeStorage(es=es_client, alias_name=alias)
        storage.bootstrap()

        tpl = es_client.indices.get_index_template(name=storage.template_name)
        assert "index_templates" in tpl or "index_template" in tpl

        write_index = storage._get_write_index()
        payload = b'{"message": "hello integration"}'
        doc_id = f"{alias}-doc-1"

        storage._set_bytes(doc_id, payload)

        # IMPORTANT: make the document visible to search
        es_client.indices.refresh(index=write_index)

        out = storage._get_bytes(doc_id)
        assert out == payload

        # Verify alias points to the index created from the template
        aliases_resp = es_client.indices.get_alias(name=alias)
        assert write_index in aliases_resp
    finally:
        for k in [
            "SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX",
            "SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN",
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS",
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA",
            "SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC",
        ]:
            os.environ.pop(k, None)


@pytest.mark.integration
def test_backend_roundtrip_over_http():
    client = make_http_client()
    try:
        wait_for_cluster(client, timeout=90)
    except TimeoutError as e:
        pytest.skip(f"HTTP cluster not ready: {e}")

    _run_backend_roundtrip(client, alias="sentry-integ-http", env_prefix="integ-http")


@pytest.mark.integration
def test_backend_roundtrip_over_https():
    client = make_https_client(with_auth=True)
    try:
        wait_for_cluster(client, timeout=120)
    except TimeoutError as e:
        pytest.skip(f"HTTPS cluster not ready: {e}")

    _run_backend_roundtrip(client, alias="sentry-integ-https", env_prefix="integ-https")
