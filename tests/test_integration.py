import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from opensearchpy import AsyncOpenSearch, NotFoundError
from opensearchpy.exceptions import (
    AuthenticationException,
    AuthorizationException,
    TransportError,
)

# Import your backend
from sentry_opensearch_nodestore import AsyncOpenSearchNodeStorage

# --- Connection details (env overridable) ---
OPENSEARCH_HOST = os.getenv("OPENSEARCH_TEST_HOST", "localhost")

OPENSEARCH_HTTP_PORT = int(os.getenv("OPENSEARCH_TEST_PORT", 9200))
OPENSEARCH_HTTPS_PORT = int(os.getenv("OPENSEARCH_TEST_HTTPS_PORT", 9201))

OPENSEARCH_HTTPS_USER = os.getenv("OPENSEARCH_TEST_HTTPS_USER", "admin")
OPENSEARCH_HTTPS_PASSWORD = os.getenv(
    "OPENSEARCH_TEST_HTTPS_PASSWORD", "myStrongPassword123!"
)

# TLS verification (defaults off for local test clusters)
VERIFY_CERTS = os.getenv("OPENSEARCH_TEST_VERIFY_CERTS", "false").lower() == "true"
CA_CERTS = os.getenv("OPENSEARCH_TEST_CA_CERTS")  # optional path to CA bundle

requires_opensearch = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "true",
    reason="RUN_INTEGRATION_TESTS environment variable is not set to 'true'",
)


# --- Fixtures ---
@pytest_asyncio.fixture
async def opensearch_client_http():
    client = AsyncOpenSearch(
        hosts=[
            {"host": OPENSEARCH_HOST, "port": OPENSEARCH_HTTP_PORT, "scheme": "http"}
        ],
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
    )
    try:
        if not await client.ping():
            pytest.fail(
                f"Could not connect to OpenSearch over HTTP on port {OPENSEARCH_HTTP_PORT}."
            )
        yield client
    finally:
        await client.close()


@pytest_asyncio.fixture
async def opensearch_client_secure():
    client = AsyncOpenSearch(
        hosts=[
            {"host": OPENSEARCH_HOST, "port": OPENSEARCH_HTTPS_PORT, "scheme": "https"}
        ],
        http_auth=(OPENSEARCH_HTTPS_USER, OPENSEARCH_HTTPS_PASSWORD),
        use_ssl=True,
        verify_certs=VERIFY_CERTS,
        ca_certs=CA_CERTS,
        ssl_assert_hostname=False if not VERIFY_CERTS else True,
        ssl_show_warn=False,
    )
    try:
        if not await client.ping():
            pytest.fail(
                f"Could not connect to secure OpenSearch over HTTPS on port {OPENSEARCH_HTTPS_PORT}."
            )
        yield client
    finally:
        await client.close()


# Helper to parametrize over both clients
@pytest_asyncio.fixture
async def opensearch_client(request, opensearch_client_http, opensearch_client_secure):
    if request.param == "opensearch_client_http":
        return opensearch_client_http
    elif request.param == "opensearch_client_secure":
        return opensearch_client_secure
    else:
        raise RuntimeError(f"Unknown client fixture param: {request.param}")


# --- Tests ---


@requires_opensearch
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "opensearch_client",
    ["opensearch_client_http", "opensearch_client_secure"],
    indirect=True,
)
async def test_connectivity_http_and_https(opensearch_client):
    # Verify ping works
    assert await opensearch_client.ping() is True

    # Fetch cluster info
    info = await opensearch_client.info()
    assert "cluster_name" in info
    assert "version" in info

    # Sanity-check we're talking to the expected port/scheme
    host = opensearch_client.transport.hosts[0]
    port = host.get("port")
    scheme = host.get(
        "scheme",
        (
            "http"
            if not getattr(opensearch_client.transport.kwargs, "use_ssl", False)
            else "https"
        ),
    )
    if port == OPENSEARCH_HTTP_PORT:
        assert scheme == "http"
    elif port == OPENSEARCH_HTTPS_PORT:
        assert scheme == "https"
        # For https client, use_ssl should be True
        assert opensearch_client.transport.kwargs.get("use_ssl") is True
    else:
        pytest.skip(f"Unknown port under test: {port}")


@requires_opensearch
@pytest.mark.asyncio
async def test_https_auth_success_and_bad_creds(opensearch_client_secure):
    # Success path with correct credentials
    assert await opensearch_client_secure.ping() is True
    info = await opensearch_client_secure.info()
    assert "cluster_name" in info

    # Failure path: wrong credentials
    bad_client = AsyncOpenSearch(
        hosts=[
            {"host": OPENSEARCH_HOST, "port": OPENSEARCH_HTTPS_PORT, "scheme": "https"}
        ],
        http_auth=(OPENSEARCH_HTTPS_USER, "definitely-wrong-password"),
        use_ssl=True,
        verify_certs=VERIFY_CERTS,
        ca_certs=CA_CERTS,
        ssl_assert_hostname=False if not VERIFY_CERTS else True,
        ssl_show_warn=False,
    )
    try:
        # ping may return False or raise depending on security config
        try:
            ping_ok = await bad_client.ping()
            assert ping_ok is False, "Ping unexpectedly succeeded with bad credentials"
        except (AuthenticationException, AuthorizationException, TransportError):
            pass  # acceptable

        # A privileged API should raise
        with pytest.raises(
            (AuthenticationException, AuthorizationException, TransportError)
        ):
            await bad_client.info()
    finally:
        await bad_client.close()


@requires_opensearch
@pytest.mark.integration
class TestOpenSearchIntegration:
    # Note: @pytest.mark.asyncio must be last (on top)
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "opensearch_client",
        ["opensearch_client_http", "opensearch_client_secure"],
        indirect=True,
    )
    async def test_full_lifecycle(self, opensearch_client):
        client_port = opensearch_client.transport.hosts[0].get("port")
        template_name = f"sentry-test-template-{client_port}"
        alias_name = f"sentry-test-alias-{client_port}"

        nodestore = AsyncOpenSearchNodeStorage(
            os_client=opensearch_client,
            template_name=template_name,
            alias_name=alias_name,
        )

        try:
            # Bootstrap template and alias
            await nodestore.bootstrap()
            template_exists = await opensearch_client.indices.exists_index_template(
                name=template_name
            )
            assert template_exists, "Bootstrap should create the index template"

            # Create document
            node_id = "integration_test_event_001"
            node_data = b'{"message": "this is a real event"}'
            await nodestore._set_bytes(node_id, node_data)
            await opensearch_client.indices.refresh(index=f"{alias_name},sentry-*")

            # Read back
            retrieved_data = await nodestore._get_bytes(node_id)
            assert (
                retrieved_data == node_data
            ), "Should retrieve the exact data that was set"

            # Delete
            await nodestore.delete(node_id)
            await opensearch_client.indices.refresh(index=f"{alias_name},sentry-*")
            retrieved_after_delete = await nodestore._get_bytes(node_id)
            assert retrieved_after_delete is None, "Data should be None after deletion"

            # Cleanup old index
            old_index_name = "sentry-2020-01-01"
            await opensearch_client.indices.create(index=old_index_name)
            await opensearch_client.indices.put_alias(
                index=old_index_name, name=alias_name
            )
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=365)
            await nodestore.cleanup(cutoff_date)
            old_index_exists = await opensearch_client.indices.exists(
                index=old_index_name
            )
            assert (
                not old_index_exists
            ), "Cleanup should delete indices older than the cutoff"

        finally:
            # Cleanup indices and template
            try:
                await opensearch_client.indices.delete(
                    index="sentry-*", ignore_unavailable=True
                )
                try:
                    await opensearch_client.indices.delete_index_template(
                        name=template_name
                    )
                except NotFoundError:
                    pass
            except Exception as e:
                print(f"CLEANUP FAILED: {e}")
