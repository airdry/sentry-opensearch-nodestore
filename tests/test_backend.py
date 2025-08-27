# tests/test_backend.py

import pytest
from unittest.mock import AsyncMock, MagicMock, ANY
from datetime import datetime, timedelta, timezone
import zlib
import base64

from opensearchpy import AsyncOpenSearch
from opensearchpy.exceptions import NotFoundError
from sentry_opensearch_nodestore import AsyncOpenSearchNodeStorage
from sentry_opensearch_nodestore.backend import SyncOpenSearchNodeStorage

# The global 'pytestmark' has been REMOVED from here.


@pytest.fixture
def mock_os_client():
    # ... (fixture is unchanged) ...
    client = MagicMock(spec=AsyncOpenSearch)
    client.index = AsyncMock()
    client.get = AsyncMock()
    client.search = AsyncMock()
    client.delete_by_query = AsyncMock()
    client.indices = MagicMock()
    client.indices.get_index_template = AsyncMock()
    client.indices.put_index_template = AsyncMock()
    client.indices.get_alias = AsyncMock()
    client.indices.delete = AsyncMock()
    return client


@pytest.fixture
def nodestore(mock_os_client):
    # ... (fixture is unchanged) ...
    return AsyncOpenSearchNodeStorage(os_client=mock_os_client)


@pytest.fixture
def sync_nodestore(mock_os_client):
    # Uses the same async-mocked client; the sync wrapper will await inside
    return SyncOpenSearchNodeStorage(os_client=mock_os_client)


class TestInitialization:
    # NOTE: No asyncio marks in this class, as the tests are synchronous.
    def test_defaults(self, monkeypatch, mock_os_client):
        monkeypatch.delenv(
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", raising=False
        )
        monkeypatch.delenv(
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS", raising=False
        )
        monkeypatch.delenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN", raising=False)
        monkeypatch.delenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC", raising=False)
        store = AsyncOpenSearchNodeStorage(os_client=mock_os_client)
        assert store.number_of_shards == 3
        assert store.number_of_replicas == 1
        assert store.index_pattern == "sentry-*"
        assert store.index_codec == "zstd"

    def test_env_vars_override(self, monkeypatch, mock_os_client):
        monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", "5")
        monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS", "2")
        monkeypatch.setenv(
            "SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN", "custom-events-*"
        )
        monkeypatch.setenv(
            "SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC", "best_compression"
        )
        store = AsyncOpenSearchNodeStorage(os_client=mock_os_client)
        assert store.number_of_shards == 5
        assert store.number_of_replicas == 2
        assert store.index_pattern == "custom-events-*"
        assert store.index_codec == "best_compression"

    def test_invalid_env_vars_fallback(self, monkeypatch, mock_os_client):
        monkeypatch.setenv(
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", "not-a-number"
        )
        monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS", "invalid")
        store = AsyncOpenSearchNodeStorage(os_client=mock_os_client)
        assert store.number_of_shards == 3
        assert store.number_of_replicas == 1


class TestBootstrap:
    @pytest.mark.asyncio
    async def test_creates_template_if_not_found(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        mock_os_client.indices.get_index_template.side_effect = NotFoundError()
        await nodestore.bootstrap()
        mock_os_client.indices.get_index_template.assert_awaited_once_with(
            name=nodestore.template_name
        )
        mock_os_client.indices.put_index_template.assert_awaited_once()
        _, kwargs = mock_os_client.indices.put_index_template.await_args
        template_body = kwargs["body"]
        assert template_body["index_patterns"] == [nodestore.index_pattern]
        settings = template_body["template"]["settings"]["index"]
        assert settings["number_of_shards"] == nodestore.number_of_shards
        assert settings["number_of_replicas"] == nodestore.number_of_replicas
        assert settings["codec"] == nodestore.index_codec

    @pytest.mark.asyncio
    async def test_does_nothing_if_template_exists(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        await nodestore.bootstrap()
        mock_os_client.indices.get_index_template.assert_awaited_once_with(
            name=nodestore.template_name
        )
        mock_os_client.indices.put_index_template.assert_not_awaited()


class TestCRUDOperations:
    NODE_ID = "event_12345"
    NODE_DATA = b'{"message": "hello world"}'

    @pytest.mark.asyncio
    async def test_set_bytes(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        await nodestore._set_bytes(self.NODE_ID, self.NODE_DATA)
        mock_os_client.index.assert_awaited_once()
        _, kwargs = mock_os_client.index.await_args
        assert kwargs["id"] == self.NODE_ID
        assert kwargs["index"].startswith("sentry-")
        body = kwargs["body"]
        compressed_data = base64.b64decode(body["data"])
        assert zlib.decompress(compressed_data) == self.NODE_DATA
        assert "timestamp" in body

    @pytest.mark.asyncio
    async def test_get_bytes_found(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        mock_os_client.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": [{"_index": "sentry-2025-08-26"}]}
        }
        compressed_b64 = base64.b64encode(zlib.compress(self.NODE_DATA)).decode("utf-8")
        mock_os_client.get.return_value = {"fields": {"data": [compressed_b64]}}
        result = await nodestore._get_bytes(self.NODE_ID)
        mock_os_client.search.assert_awaited_once_with(
            index=nodestore.alias_name, body=ANY
        )
        mock_os_client.get.assert_awaited_once_with(
            id=self.NODE_ID, index="sentry-2025-08-26", stored_fields=["data"]
        )
        assert result == self.NODE_DATA

    @pytest.mark.asyncio
    async def test_get_bytes_not_found(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        mock_os_client.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": [{"_index": "sentry-2025-08-26"}]}
        }
        mock_os_client.get.side_effect = NotFoundError()
        result = await nodestore._get_bytes(self.NODE_ID)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_bytes_index_not_found(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        mock_os_client.search.return_value = {
            "hits": {"total": {"value": 0}, "hits": []}
        }
        result = await nodestore._get_bytes(self.NODE_ID)
        assert result is None
        mock_os_client.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delete(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        await nodestore.delete(self.NODE_ID)
        expected_query = {"query": {"term": {"_id": self.NODE_ID}}}
        mock_os_client.delete_by_query.assert_awaited_once_with(
            index=nodestore.alias_name,
            body=expected_query,
            refresh=ANY,
            wait_for_completion=ANY,
        )

    @pytest.mark.asyncio
    async def test_delete_multi(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        id_list = ["id1", "id2"]
        await nodestore.delete_multi(id_list)
        expected_query = {"query": {"ids": {"values": id_list}}}
        mock_os_client.delete_by_query.assert_awaited_once_with(
            index=nodestore.alias_name,
            body=expected_query,
            refresh=ANY,
            wait_for_completion=ANY,
        )

    @pytest.mark.asyncio
    async def test_delete_multi_empty_list(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        await nodestore.delete_multi([])
        mock_os_client.delete_by_query.assert_not_awaited()


class TestCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_deletes_old_indices(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        old_index_date = (cutoff - timedelta(days=1)).strftime("%Y-%m-%d")
        new_index_date = (cutoff + timedelta(days=1)).strftime("%Y-%m-%d")
        old_index = f"sentry-{old_index_date}"
        new_index = f"sentry-{new_index_date}"
        mock_os_client.indices.get_alias.return_value = {
            old_index: {"aliases": {nodestore.alias_name: {}}},
            new_index: {"aliases": {nodestore.alias_name: {}}},
            "sentry-2025-01-01-reindexed": {"aliases": {nodestore.alias_name: {}}},
        }
        await nodestore.cleanup(cutoff)
        mock_os_client.indices.get_alias.assert_awaited_once_with(
            name=nodestore.alias_name
        )
        assert mock_os_client.indices.delete.await_count == 2
        mock_os_client.indices.delete.assert_any_await(index=old_index)
        mock_os_client.indices.delete.assert_any_await(
            index="sentry-2025-01-01-reindexed"
        )

    @pytest.mark.asyncio
    async def test_cleanup_handles_no_alias(self, nodestore, mock_os_client):
        # ... (test implementation is unchanged) ...
        mock_os_client.indices.get_alias.side_effect = NotFoundError()
        await nodestore.cleanup(datetime.now(timezone.utc))
        mock_os_client.indices.delete.assert_not_awaited()


class TestSyncWrapperBootstrap:
    def test_creates_template_if_not_found(self, sync_nodestore, mock_os_client):
        mock_os_client.indices.get_index_template.side_effect = NotFoundError()

        sync_nodestore.bootstrap()

        mock_os_client.indices.get_index_template.assert_awaited_once_with(
            name=sync_nodestore.template_name
        )
        mock_os_client.indices.put_index_template.assert_awaited_once()
        _, kwargs = mock_os_client.indices.put_index_template.await_args
        template_body = kwargs["body"]
        assert template_body["index_patterns"] == [sync_nodestore.index_pattern]
        settings = template_body["template"]["settings"]["index"]
        assert settings["number_of_shards"] == sync_nodestore.number_of_shards
        assert settings["number_of_replicas"] == sync_nodestore.number_of_replicas
        assert settings["codec"] == sync_nodestore.index_codec

    def test_does_nothing_if_template_exists(self, sync_nodestore, mock_os_client):
        sync_nodestore.bootstrap()
        mock_os_client.indices.get_index_template.assert_awaited_once_with(
            name=sync_nodestore.template_name
        )
        mock_os_client.indices.put_index_template.assert_not_awaited()


class TestSyncWrapperCRUD:
    NODE_ID = "event_12345"
    NODE_DATA = b'{"message": "hello world"}'

    def test_set_bytes(self, sync_nodestore, mock_os_client):
        sync_nodestore._set_bytes(self.NODE_ID, self.NODE_DATA)

        mock_os_client.index.assert_awaited_once()
        _, kwargs = mock_os_client.index.await_args
        assert kwargs["id"] == self.NODE_ID
        assert kwargs["index"].startswith("sentry-")
        body = kwargs["body"]
        compressed_data = base64.b64decode(body["data"])
        assert zlib.decompress(compressed_data) == self.NODE_DATA
        assert "timestamp" in body

    def test_get_bytes_found(self, sync_nodestore, mock_os_client):
        mock_os_client.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": [{"_index": "sentry-2025-08-26"}]}
        }
        compressed_b64 = base64.b64encode(zlib.compress(self.NODE_DATA)).decode("utf-8")
        mock_os_client.get.return_value = {"fields": {"data": [compressed_b64]}}

        result = sync_nodestore._get_bytes(self.NODE_ID)

        mock_os_client.search.assert_awaited_once_with(
            index=sync_nodestore.alias_name, body=ANY
        )
        mock_os_client.get.assert_awaited_once_with(
            id=self.NODE_ID, index="sentry-2025-08-26", stored_fields=["data"]
        )
        assert result == self.NODE_DATA

    def test_get_bytes_not_found(self, sync_nodestore, mock_os_client):
        mock_os_client.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": [{"_index": "sentry-2025-08-26"}]}
        }
        mock_os_client.get.side_effect = NotFoundError()

        result = sync_nodestore._get_bytes(self.NODE_ID)

        assert result is None

    def test_get_bytes_index_not_found(self, sync_nodestore, mock_os_client):
        mock_os_client.search.return_value = {
            "hits": {"total": {"value": 0}, "hits": []}
        }

        result = sync_nodestore._get_bytes(self.NODE_ID)

        assert result is None
        mock_os_client.get.assert_not_awaited()

    def test_delete(self, sync_nodestore, mock_os_client):
        sync_nodestore.delete(self.NODE_ID)

        expected_query = {"query": {"term": {"_id": self.NODE_ID}}}
        mock_os_client.delete_by_query.assert_awaited_once_with(
            index=sync_nodestore.alias_name,
            body=expected_query,
            refresh=ANY,
            wait_for_completion=ANY,
        )

    def test_delete_multi(self, sync_nodestore, mock_os_client):
        id_list = ["id1", "id2"]

        sync_nodestore.delete_multi(id_list)

        expected_query = {"query": {"ids": {"values": id_list}}}
        mock_os_client.delete_by_query.assert_awaited_once_with(
            index=sync_nodestore.alias_name,
            body=expected_query,
            refresh=ANY,
            wait_for_completion=ANY,
        )

    def test_delete_multi_empty_list(self, sync_nodestore, mock_os_client):
        sync_nodestore.delete_multi([])

        mock_os_client.delete_by_query.assert_not_awaited()


class TestSyncWrapperCleanup:
    def test_cleanup_deletes_old_indices(self, sync_nodestore, mock_os_client):
        cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        old_index_date = (cutoff - timedelta(days=1)).strftime("%Y-%m-%d")
        new_index_date = (cutoff + timedelta(days=1)).strftime("%Y-%m-%d")
        old_index = f"sentry-{old_index_date}"
        new_index = f"sentry-{new_index_date}"

        mock_os_client.indices.get_alias.return_value = {
            old_index: {"aliases": {sync_nodestore.alias_name: {}}},
            new_index: {"aliases": {sync_nodestore.alias_name: {}}},
            "sentry-2025-01-01-reindexed": {"aliases": {sync_nodestore.alias_name: {}}},
        }

        sync_nodestore.cleanup(cutoff)

        mock_os_client.indices.get_alias.assert_awaited_once_with(
            name=sync_nodestore.alias_name
        )
        assert mock_os_client.indices.delete.await_count == 2
        mock_os_client.indices.delete.assert_any_await(index=old_index)
        mock_os_client.indices.delete.assert_any_await(
            index="sentry-2025-01-01-reindexed"
        )

    def test_cleanup_handles_no_alias(self, sync_nodestore, mock_os_client):
        mock_os_client.indices.get_alias.side_effect = NotFoundError()

        sync_nodestore.cleanup(datetime.now(timezone.utc))

        mock_os_client.indices.delete.assert_not_awaited()
