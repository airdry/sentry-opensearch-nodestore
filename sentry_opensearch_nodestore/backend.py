# sentry_opensearch_nodestore/backend.py

import os
import json
import base64
import logging
import zlib
from datetime import datetime, timezone

from opensearchpy import exceptions
from sentry.nodestore.base import NodeStorage


def _read_env_int(name: str, default: int) -> int:
    """
    Read an integer environment variable (uppercase only) with a fallback default.
    Raises ValueError if the variable is set but not an integer.
    """
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    try:
        return int(val)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer, got: {val!r}") from e


def _parse_single_index_pattern_from_env() -> list[str]:
    """
    Read exactly one index pattern from SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN.
    Accepts:
      - A plain string like "sentry-*"
      - A JSON array with exactly one item
      - A comma-separated list that must resolve to exactly one item

    Returns a list with one string (OpenSearch expects a list of patterns).
    Raises ValueError if multiple or invalid values are provided.
    """
    env_name = "SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN"
    raw = os.getenv(env_name)

    if raw is None or raw.strip() == "":
        return ["sentry-*"]

    raw = raw.strip()

    if raw.startswith("["):
        try:
            arr = json.loads(raw)
        except Exception as e:
            raise ValueError(
                f"{env_name} must be a single pattern string or a JSON array with one string"
            ) from e
        if not isinstance(arr, list) or not arr:
            raise ValueError(f"{env_name} must be a non-empty JSON array")
        if len(arr) != 1:
            raise ValueError(
                f"{env_name} must contain exactly one pattern; got {len(arr)}"
            )
        value = str(arr[0]).strip()
        if not value:
            raise ValueError(f"{env_name} contains an empty pattern")
        return [value]

    if "," in raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) != 1:
            raise ValueError(
                f"{env_name} must contain exactly one pattern (no multiple values)"
            )
        return [parts[0]]

    return [raw]


class OpenSearchNodeStorage(NodeStorage):
    """
    A Sentry NodeStorage implementation backed by OpenSearch.

    Environment variables (uppercase only):
      - SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS (default: 3)
      - SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA (default: 1)
      - SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN (default: "sentry-*", must be single value)
      - SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC (default: "zstd")
      - SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX (optional; when set, index = "sentry-<prefix>-{date}")

    Index name behavior:
      - Constructor default index="sentry-{prefix}-{date}"
      - If SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX is set (non-empty after strip),
        resolves to "sentry-<prefix>-{date}".
      - If not set/empty, resolves to "sentry-{date}".
      - If a custom index string is passed to __init__ (not equal to the default pattern),
        it is used as-is.
    """

    logger = logging.getLogger("sentry.nodestore.opensearch")
    encoding = "utf-8"

    def __init__(
        self,
        es,
        index: str = "sentry-{prefix}-{date}",
        refresh: bool = False,
        template_name: str = "sentry",
        alias_name: str = "sentry",
        validate_es: bool = False,
    ):
        self.es = es
        self.refresh = refresh
        self.template_name = template_name
        self.alias_name = alias_name
        self.validate_es = validate_es

        # Optional index prefix from env; if not set -> no prefix
        raw_prefix = os.getenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX", "")
        self.index_prefix = raw_prefix.strip() or None

        # Resolve the index template:
        # - If env prefix is present -> "sentry-<prefix>-{date}"
        # - Otherwise -> "sentry-{date}"
        # If a custom index value was passed and doesn't match the default placeholder,
        # we keep it as-is.
        if index == "sentry-{prefix}-{date}":
            if self.index_prefix:
                self.index = f"sentry-{self.index_prefix}-" + "{date}"
            else:
                self.index = "sentry-{date}"
        else:
            self.index = index

        # Uppercase env vars only
        self.number_of_shards = _read_env_int(
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", 3
        )
        self.number_of_replicas = _read_env_int(
            "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA", 1
        )

        # Index codec (default to zstd if not set or empty)
        codec = os.getenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC", "").strip()
        self.index_codec = codec or "zstd"

        # Index pattern: must be exactly one
        self.index_patterns = _parse_single_index_pattern_from_env()
        if "*" not in self.index_patterns[0]:
            # Not required, but most patterns include a wildcard
            self.logger.warning(
                "index_pattern.missing_wildcard",
                extra={"pattern": self.index_patterns[0]},
            )

        super(OpenSearchNodeStorage, self).__init__()

    def bootstrap(self):
        """
        Ensure that a composable index template exists for Sentry nodestore documents.
        """
        try:
            self.es.indices.get_index_template(name=self.template_name)
            self.logger.info(
                "bootstrap.template.check",
                extra={"template": self.template_name, "status": "exists"},
            )
        except exceptions.NotFoundError:
            self.logger.info(
                "bootstrap.template.check",
                extra={"template": self.template_name, "status": "not found"},
            )

            body = {
                "index_patterns": self.index_patterns,  # exactly one, as a list
                "template": {
                    "settings": {
                        "index": {
                            "codec": self.index_codec,
                            "number_of_shards": self.number_of_shards,
                            "number_of_replicas": self.number_of_replicas,
                        }
                    },
                    "mappings": {
                        "_source": {"enabled": False},
                        "dynamic": False,
                        "properties": {
                            "data": {"type": "keyword", "index": False, "store": True},
                            "timestamp": {"type": "date", "store": True},
                        },
                    },
                    "aliases": {self.alias_name: {}},
                },
            }

            self.es.indices.put_index_template(
                name=self.template_name,
                body=body,
                create=True,  # fail if it somehow races and already exists
            )

            self.logger.info(
                "bootstrap.template.create",
                extra={"template": self.template_name, "alias": self.alias_name},
            )

    def _get_write_index(self) -> str:
        """
        Resolve the actual write index for today, e.g., "sentry-2025-08-28" or "sentry-prod-2025-08-28".
        """
        return self.index.format(date=datetime.today().strftime("%Y-%m-%d"))

    def _get_read_index(self, id: str) -> str | None:
        """
        Locate the backing index for a given document ID by searching through the alias.
        Returns the index name if found; otherwise None.
        """
        search = self.es.search(
            index=self.alias_name,
            body={
                "query": {
                    "term": {"_id": id},
                },
            },
        )
        if search["hits"]["total"]["value"] == 1:
            return search["hits"]["hits"][0]["_index"]
        else:
            return None

    def _compress(self, data: bytes) -> str:
        """
        Compress arbitrary bytes with zlib and base64-encode to a UTF-8 string.
        """
        return base64.b64encode(zlib.compress(data)).decode(self.encoding)

    def _decompress(self, data: str) -> bytes:
        """
        Base64-decode and zlib-decompress into original bytes.
        """
        return zlib.decompress(base64.b64decode(data))

    def delete(self, id: str):
        """
        Delete a single node by id via alias.
        >>> nodestore.delete('key1')
        """
        try:
            self.logger.info("document.delete.executed", extra={"doc_id": id})
            self.es.delete_by_query(
                index=self.alias_name, body={"query": {"term": {"_id": id}}}
            )
        except exceptions.NotFoundError:
            pass
        except exceptions.ConflictError:
            pass

    def delete_multi(self, id_list: list[str]):
        """
        Delete multiple nodes.
        Note: This is not guaranteed to be atomic and may result in a partial delete.
        >>> nodestore.delete_multi(['key1', 'key2'])
        """
        try:
            response = self.es.delete_by_query(
                index=self.alias_name, body={"query": {"ids": {"values": id_list}}}
            )
            self.logger.info(
                "document.delete_multi.executed",
                extra={
                    "docs_to_delete": len(id_list),
                    "docs_deleted": response.get("deleted", 0),
                },
            )
        except exceptions.NotFoundError:
            pass
        except exceptions.ConflictError:
            pass

    def _get_bytes(self, id: str) -> bytes | None:
        """
        Fetch the stored 'data' field for a given id, returning bytes or None if not found.
        >>> nodestore._get_bytes('key1')
        b'{"message": "hello world"}'
        """
        index = self._get_read_index(id)

        if index is not None:
            try:
                response = self.es.get(id=id, index=index, stored_fields=["data"])
            except exceptions.NotFoundError:
                return None
            else:
                return self._decompress(
                    response["fields"]["data"][0].encode(self.encoding)
                )
        else:
            self.logger.warning(
                "document.get.warning",
                extra={"doc_id": id, "error": "index containing doc_id not found"},
            )
            return None

    def _set_bytes(self, id: str, data: bytes, ttl=None):
        """
        Store bytes for a given id. TTL is currently ignored (kept for interface compatibility).
        >>> nodestore.set('key1', b"{'foo': 'bar'}")
        """
        index = self._get_write_index()
        self.es.index(
            index=index,
            id=id,
            body={
                "data": self._compress(data),
                # Use a timezone-aware UTC datetime (Python 3.13 deprecates utcnow())
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            refresh=self.refresh,
        )

    def cleanup(self, cutoff: datetime):
        """
        Delete old daily indices behind the alias if their date is older than cutoff.
        Index names are expected to look like 'sentry-YYYY-MM-DD' or 'sentry-<prefix>-YYYY-MM-DD'
        optionally with a postfix (e.g., '-fixed', '-reindex').
        """
        for index in self.es.indices.get_alias(name=self.alias_name):
            # Parse date from manually changed indices after reindex
            # (they may have postfixes like '-fixed' or '-reindex')
            index_date = "-".join(index.split("-")[1:4])
            try:
                index_ts = datetime.strptime(index_date, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                self.logger.info(
                    "index.delete.skip",
                    extra={
                        "index": index,
                        "reason": "unable to parse date",
                    },
                )
                continue

            if index_ts < cutoff:
                try:
                    self.es.indices.delete(index=index)
                except exceptions.NotFoundError:
                    self.logger.info(
                        "index.delete.error",
                        extra={"index": index, "error": "not found"},
                    )
                else:
                    self.logger.info(
                        "index.delete.executed",
                        extra={
                            "index": index,
                            "index_ts": index_ts.timestamp(),
                            "cutoff_ts": cutoff.timestamp(),
                            "status": "deleted",
                        },
                    )
