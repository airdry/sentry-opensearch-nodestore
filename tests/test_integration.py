# tests/test_integration.py

import pytest
import pytest_asyncio
import os
from datetime import datetime, timedelta, timezone

from opensearchpy import AsyncOpenSearch, NotFoundError
from sentry_opensearch_nodestore import AsyncOpenSearchNodeStorage

# ... (top of file is unchanged) ...
OPENSEARCH_TEST_HOST = os.getenv("OPENSEARCH_TEST_HOST", "localhost")
OPENSEARCH_TEST_PORT = int(os.getenv("OPENSEARCH_TEST_PORT", 9200))
requires_opensearch = pytest.mark.skipif(os.getenv("RUN_INTEGRATION_TESTS") != "true", reason="RUN_INTEGRATION_TESTS environment variable is not set to 'true'")

@pytest_asyncio.fixture
async def opensearch_client():
    client = AsyncOpenSearch(hosts=[{'host': OPENSEARCH_TEST_HOST, 'port': OPENSEARCH_TEST_PORT}])
    if not await client.ping():
        pytest.fail("Could not connect to OpenSearch. Is the Docker container running?")
    yield client
    await client.close()


@requires_opensearch
@pytest.mark.integration
class TestOpenSearchIntegration:
    @pytest.mark.asyncio
    async def test_full_lifecycle(self, opensearch_client):
        # ... (main test logic is unchanged) ...
        template_name = "sentry-test-template"
        alias_name = "sentry-test-alias"
        nodestore = AsyncOpenSearchNodeStorage(os_client=opensearch_client, template_name=template_name, alias_name=alias_name)
        try:
            await nodestore.bootstrap()
            template_exists = await opensearch_client.indices.exists_index_template(name=template_name)
            assert template_exists, "Bootstrap should create the index template"
            node_id = "integration_test_event_001"
            node_data = b'{"message": "this is a real event"}'
            await nodestore._set_bytes(node_id, node_data)
            await opensearch_client.indices.refresh(index=f"{alias_name},sentry-*")
            retrieved_data = await nodestore._get_bytes(node_id)
            assert retrieved_data == node_data, "Should retrieve the exact data that was set"
            await nodestore.delete(node_id)
            await opensearch_client.indices.refresh(index=f"{alias_name},sentry-*")
            retrieved_after_delete = await nodestore._get_bytes(node_id)
            assert retrieved_after_delete is None, "Data should be None after deletion"
            old_index_name = "sentry-2020-01-01"
            await opensearch_client.indices.create(index=old_index_name)
            await opensearch_client.indices.put_alias(index=old_index_name, name=alias_name)
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=365)
            await nodestore.cleanup(cutoff_date)
            old_index_exists = await opensearch_client.indices.exists(index=old_index_name)
            assert not old_index_exists, "Cleanup should delete indices older than the cutoff"
        finally:
            # VVVVVVVVVVVVVVVVVVVV FIX IS HERE VVVVVVVVVVVVVVVVVVVV
            try:
                await opensearch_client.indices.delete(index="sentry-*", ignore_unavailable=True)
                # Wrap the call in a try/except block to handle missing templates
                try:
                    await opensearch_client.indices.delete_index_template(name=template_name)
                except NotFoundError:
                    pass # This is expected if the test failed before template creation
            except Exception as e:
                # Log any unexpected cleanup errors but don't fail the test on them
                print(f"CLEANUP FAILED: {e}")
            # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
