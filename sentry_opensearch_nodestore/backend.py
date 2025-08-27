# sentry_opensearch_nodestore/backend.py

import base64
from datetime import datetime, timezone
import logging  # Ensure logging is imported
import zlib
import os
import opensearchpy
from opensearchpy import AsyncOpenSearch
from sentry.nodestore.base import NodeStorage


class AsyncOpenSearchNodeStorage(NodeStorage):
    """
    An asynchronous Sentry NodeStorage backend for OpenSearch.
    """

    # VVVVVVVVVVVVVVVVVVVV THIS LINE WAS MISSING VVVVVVVVVVVVVVVVVVVV
    logger = logging.getLogger("sentry.nodestore.opensearch")
    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    encoding = "utf-8"

    def __init__(
        self,
        os_client: AsyncOpenSearch,
        index: str = "sentry-{date}",
        refresh: bool = False,
        template_name: str = "sentry",
        alias_name: str = "sentry",
    ):
        self.os = os_client
        self.index = index
        self.refresh = refresh
        self.template_name = template_name
        self.alias_name = alias_name
        default_shards = 3
        default_replicas = 1

        try:
            # Ensure this variable name is exactly correct
            shards_str = os.getenv(
                "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", str(default_shards)
            )
            self.number_of_shards = int(shards_str)
        except (ValueError, TypeError):
            self.logger.warning(
                "Invalid value for SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS. Falling back to default: %d",
                default_shards,
            )
            self.number_of_shards = default_shards

        try:
            # Ensure this variable name is exactly correct
            replicas_str = os.getenv(
                "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS", str(default_replicas)
            )
            self.number_of_replicas = int(replicas_str)
        except (ValueError, TypeError):
            self.logger.warning(
                "Invalid value for SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICAS. Falling back to default: %d",
                default_replicas,
            )
            self.number_of_replicas = default_replicas

        default_pattern = "sentry-*"
        # Ensure this variable name is exactly correct
        pattern_str = os.getenv(
            "SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN", default_pattern
        )
        self.index_pattern = (
            pattern_str.strip()
            if pattern_str and pattern_str.strip()
            else default_pattern
        )

        default_codec = "zstd"
        # Ensure this variable name is exactly correct
        codec_str = os.getenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC", default_codec)
        self.index_codec = (
            codec_str.strip() if codec_str and codec_str.strip() else default_codec
        )

        super().__init__()

    async def bootstrap(self):
        """Creates the index template if it does not already exist."""
        try:
            await self.os.indices.get_index_template(name=self.template_name)
            self.logger.info(
                "bootstrap.template.check",
                extra={"template": self.template_name, "status": "exists"},
            )
        except opensearchpy.exceptions.NotFoundError:
            self.logger.info(
                "bootstrap.template.check",
                extra={"template": self.template_name, "status": "not found"},
            )
            self.logger.info(
                "bootstrap.template.settings",
                extra={
                    "shards": self.number_of_shards,
                    "replicas": self.number_of_replicas,
                    "index_pattern": self.index_pattern,
                    "codec": self.index_codec,
                },
            )
            template_body = {
                "index_patterns": [self.index_pattern],
                "template": {
                    "settings": {
                        "index": {
                            "number_of_shards": self.number_of_shards,
                            "number_of_replicas": self.number_of_replicas,
                            "codec": self.index_codec,
                        }
                    },
                    "mappings": {
                        "_source": {"enabled": False},
                        "dynamic": "false",
                        "dynamic_templates": [],
                        "properties": {
                            "data": {
                                "type": "binary",
                                "doc_values": False,
                                "store": True,
                            },
                            "timestamp": {"type": "date", "store": True},
                        },
                    },
                    "aliases": {self.alias_name: {}},
                },
            }
            await self.os.indices.put_index_template(
                name=self.template_name, body=template_body, create=True
            )
            self.logger.info(
                "bootstrap.template.create",
                extra={"template": self.template_name, "alias": self.alias_name},
            )

    def _get_write_index(self) -> str:
        return self.index.format(date=datetime.today().strftime("%Y-%m-%d"))

    async def _get_read_index(self, id: str) -> str | None:
        try:
            search_result = await self.os.search(
                index=self.alias_name, body={"query": {"term": {"_id": id}}}
            )
            if search_result["hits"]["total"]["value"] == 1:
                return search_result["hits"]["hits"][0]["_index"]
            return None
        except opensearchpy.exceptions.NotFoundError:
            return None

    def _compress(self, data: bytes) -> str:
        return base64.b64encode(zlib.compress(data)).decode(self.encoding)

    def _decompress(self, data: str) -> bytes:
        return zlib.decompress(base64.b64decode(data))

    async def delete(self, id: str):
        try:
            await self.os.delete_by_query(
                index=self.alias_name,
                body={"query": {"term": {"_id": id}}},
                refresh=self.refresh,
                wait_for_completion=True,
            )
        except (
            opensearchpy.exceptions.NotFoundError,
            opensearchpy.exceptions.ConflictError,
        ):
            pass

    async def delete_multi(self, id_list: list[str]):
        if not id_list:
            return
        try:
            await self.os.delete_by_query(
                index=self.alias_name,
                body={"query": {"ids": {"values": id_list}}},
                refresh=self.refresh,
                wait_for_completion=True,
            )
        except (
            opensearchpy.exceptions.NotFoundError,
            opensearchpy.exceptions.ConflictError,
        ):
            pass

    async def _get_bytes(self, id: str) -> bytes | None:
        index = await self._get_read_index(id)
        if index is not None:
            try:
                response = await self.os.get(id=id, index=index, stored_fields=["data"])
                return self._decompress(response["fields"]["data"][0])
            except opensearchpy.exceptions.NotFoundError:
                return None
        self.logger.warning(
            "document.get.warning",
            extra={"doc_id": id, "error": "index containing doc_id not found"},
        )
        return None

    async def _set_bytes(self, id: str, data: bytes, ttl=None):
        index = self._get_write_index()
        document_body = {
            "data": self._compress(data),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.os.index(
            id=id, index=index, body=document_body, refresh=self.refresh
        )

    async def cleanup(self, cutoff: datetime):
        try:
            aliases_response = await self.os.indices.get_alias(name=self.alias_name)
        except opensearchpy.exceptions.NotFoundError:
            self.logger.warning(
                "cleanup.alias.not_found", extra={"alias": self.alias_name}
            )
            return
        for index in aliases_response.keys():
            try:
                index_date_str = "-".join(index.split("-")[1:4])
                index_ts = datetime.strptime(index_date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except (ValueError, IndexError):
                self.logger.warning("cleanup.index.parse_error", extra={"index": index})
                continue
            if index_ts < cutoff:
                try:
                    await self.os.indices.delete(index=index)
                except opensearchpy.exceptions.NotFoundError:
                    self.logger.info(
                        "index.delete.error",
                        extra={"index": index, "error": "not found"},
                    )
