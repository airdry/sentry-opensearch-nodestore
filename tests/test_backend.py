# tests/test_backend.py
from datetime import datetime, timezone

import pytest
from opensearchpy import exceptions

# Adjust this import path if needed
from sentry_opensearch_nodestore.backend import OpenSearchNodeStorage

# Fixed timestamp for deterministic tests
FIXED_DT = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
FIXED_TS_ISO = FIXED_DT.isoformat()


class FakeIndices:
    def __init__(self):
        self.templates = {}  # name -> body
        self.put_calls = []  # list of (name, body, create)
        self.deleted_indices = []
        # alias_name -> set(index_names)
        self.alias_map = {}

    def get_index_template(self, name):
        if name in self.templates:
            return {
                "index_templates": [
                    {"name": name, "index_template": self.templates[name]}
                ]
            }
        raise exceptions.NotFoundError(
            404, "resource_not_found_exception", f"index template [{name}] not found"
        )

    def put_index_template(self, name, body, create=True):
        if create and name in self.templates:
            raise exceptions.ConflictError(
                409,
                "resource_already_exists_exception",
                f"index template [{name}] exists",
            )
        self.templates[name] = body
        self.put_calls.append((name, body, create))
        return {"acknowledged": True}

    def get_alias(self, name):
        indices = self.alias_map.get(name, set())
        return {idx: {"aliases": {name: {}}} for idx in indices}

    def delete(self, index):
        self.deleted_indices.append(index)
        for _, idxs in self.alias_map.items():
            idxs.discard(index)
        return {"acknowledged": True}


class FakeES:
    def __init__(self):
        self.indices = FakeIndices()
        # storage: index -> { id -> body }
        self.storage = {}
        self.delete_by_query_calls = []  # (index, body)

    def index(self, index, id, body, refresh=False):
        self.storage.setdefault(index, {})
        self.storage[index][id] = body
        return {"_index": index, "_id": id, "result": "created"}

    def get(self, id, index, stored_fields=None):
        try:
            payload = self.storage[index][id]
        except KeyError:
            raise exceptions.NotFoundError(
                404, "not_found", f"doc not found [{index}/{id}]"
            )
        fields = {}
        stored_fields = stored_fields or []
        for f in stored_fields:
            if f in payload:
                fields[f] = [payload[f]]
        return {"_index": index, "_id": id, "fields": fields}

    def delete_by_query(self, index, body):
        self.delete_by_query_calls.append((index, body))
        return {"deleted": 0}

    def search(self, index, body):
        term = body.get("query", {}).get("term", {})
        doc_id = term.get("_id")

        if index in self.indices.alias_map:
            indices = list(self.indices.alias_map[index])
        else:
            indices = [index]

        for idx in indices:
            if idx in self.storage and doc_id in self.storage[idx]:
                return {
                    "hits": {
                        "total": {"value": 1, "relation": "eq"},
                        "hits": [{"_index": idx, "_id": doc_id}],
                    }
                }

        return {"hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}}


@pytest.fixture
def es():
    return FakeES()


@pytest.fixture(autouse=True)
def fixed_timestamp(monkeypatch):
    """
    Autouse fixture that patches _set_bytes to always use a fixed ISO timestamp.
    Avoids datetime.utcnow() deprecation warnings and makes tests deterministic.
    """

    def _patched_set_bytes(self, id, data, ttl=None):
        index = self._get_write_index()
        self.es.index(
            index=index,
            id=id,
            body={
                "data": self._compress(data),
                "timestamp": FIXED_TS_ISO,
            },
            refresh=self.refresh,
        )

    monkeypatch.setattr(OpenSearchNodeStorage, "_set_bytes", _patched_set_bytes)
    yield


def clear_env(monkeypatch):
    keys = [
        "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS",
        "SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA",
        "SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN",
        "SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC",
        "SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX",
    ]
    for k in keys:
        monkeypatch.delenv(k, raising=False)


# -------- Init/index prefix tests --------


def test_index_default_without_prefix(monkeypatch, es):
    clear_env(monkeypatch)
    storage = OpenSearchNodeStorage(es=es)  # default constructor
    # When no prefix env is set, index should resolve to "sentry-{date}"
    assert storage.index == "sentry-{date}"
    # And _get_write_index should produce a date-only index
    today = datetime.today().strftime("%Y-%m-%d")
    assert storage._get_write_index() == f"sentry-{today}"


def test_index_with_prefix_env(monkeypatch, es):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX", "prod")
    storage = OpenSearchNodeStorage(es=es)  # default constructor
    # With prefix set, index template should include it
    assert storage.index == "sentry-prod-{date}"
    today = datetime.today().strftime("%Y-%m-%d")
    assert storage._get_write_index() == f"sentry-prod-{today}"


def test_index_with_prefix_env_trim(monkeypatch, es):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX", "  foo  ")
    storage = OpenSearchNodeStorage(es=es)
    assert storage.index == "sentry-foo-{date}"
    today = datetime.today().strftime("%Y-%m-%d")
    assert storage._get_write_index() == f"sentry-foo-{today}"


def test_index_with_empty_prefix_behaves_as_default(monkeypatch, es):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PREFIX", "")
    storage = OpenSearchNodeStorage(es=es)
    assert storage.index == "sentry-{date}"


# -------- Env parsing and codec tests --------


def test_env_defaults(monkeypatch, es, caplog):
    clear_env(monkeypatch)
    storage = OpenSearchNodeStorage(es=es)
    assert storage.number_of_shards == 3
    assert storage.number_of_replicas == 1
    assert storage.index_patterns == ["sentry-*"]
    assert storage.index_codec == "zstd"
    assert not any(
        "index_pattern.missing_wildcard" in r.message for r in caplog.records
    )


def test_env_valid_values(monkeypatch, es):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", "5")
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA", "2")
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN", "my-sentry-*")
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_CODEC", "best_compression")

    storage = OpenSearchNodeStorage(es=es)
    assert storage.number_of_shards == 5
    assert storage.number_of_replicas == 2
    assert storage.index_patterns == ["my-sentry-*"]
    assert storage.index_codec == "best_compression"


def test_env_invalid_shards_raises(monkeypatch, es):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_SHARDS", "abc")
    with pytest.raises(ValueError, match="NUMBER_OF_SHARDS"):
        OpenSearchNodeStorage(es=es)


def test_env_invalid_replicas_raises(monkeypatch, es):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_NUMBER_OF_REPLICA", "xyz")
    with pytest.raises(ValueError, match="NUMBER_OF_REPLICA"):
        OpenSearchNodeStorage(es=es)


@pytest.mark.parametrize("value", ['["s1","s2"]', "s1,s2"])
def test_env_index_pattern_multiple_raises(monkeypatch, es, value):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN", value)
    with pytest.raises(ValueError, match="exactly one pattern"):
        OpenSearchNodeStorage(es=es)


def test_env_index_pattern_no_wildcard_logs_warning(monkeypatch, es, caplog):
    clear_env(monkeypatch)
    monkeypatch.setenv("SENTRY_NODESTORE_OPENSEARCH_INDEX_PATTERN", "sentry")  # no '*'
    storage = OpenSearchNodeStorage(es=es)
    assert storage.index_patterns == ["sentry"]
    assert any(r.message == "index_pattern.missing_wildcard" for r in caplog.records)


# -------- Bootstrap, set/get, delete, cleanup --------


def test_bootstrap_creates_template_with_expected_body(es):
    storage = OpenSearchNodeStorage(es=es)

    # Ensure config flows into template
    storage.number_of_shards = 7
    storage.number_of_replicas = 4
    storage.index_patterns = ["only-one-*"]
    storage.index_codec = "best_compression"

    storage.bootstrap()

    assert es.indices.put_calls, "Expected put_index_template to be called"
    name, body, create = es.indices.put_calls[-1]
    assert name == storage.template_name
    assert create is True

    assert body["index_patterns"] == ["only-one-*"]
    tpl = body["template"]
    idx_settings = tpl["settings"]["index"]
    assert idx_settings["number_of_shards"] == 7
    assert idx_settings["number_of_replicas"] == 4
    assert idx_settings["codec"] == "best_compression"

    assert tpl["mappings"]["_source"]["enabled"] is False
    assert tpl["mappings"]["dynamic"] is False
    assert "data" in tpl["mappings"]["properties"]
    assert tpl["mappings"]["properties"]["data"]["type"] in ("keyword",)
    assert tpl["mappings"]["properties"]["data"]["index"] is False
    assert tpl["aliases"].get(storage.alias_name) == {}


def test_bootstrap_noop_when_template_exists(es):
    es.indices.templates["sentry"] = {"index_patterns": ["sentry-*"], "template": {}}
    storage = OpenSearchNodeStorage(es=es)
    storage.bootstrap()
    assert not es.indices.put_calls, "put_index_template should not be called"


def test_set_and_get_bytes_roundtrip(es):
    storage = OpenSearchNodeStorage(es=es, alias_name="sentry")
    write_index = storage._get_write_index()
    es.indices.alias_map.setdefault("sentry", set()).add(write_index)

    payload = b'{"message": "hello"}'
    doc_id = "abc123"

    storage._set_bytes(doc_id, payload)
    out = storage._get_bytes(doc_id)

    assert out == payload
    assert es.storage[write_index][doc_id]["timestamp"] == FIXED_TS_ISO


def test_delete_queries(es):
    storage = OpenSearchNodeStorage(es=es, alias_name="sentry")
    storage.delete("id1")
    storage.delete_multi(["a", "b"])

    assert len(es.delete_by_query_calls) == 2

    idx1, q1 = es.delete_by_query_calls[0]
    assert idx1 == "sentry"
    assert q1 == {"query": {"term": {"_id": "id1"}}}

    idx2, q2 = es.delete_by_query_calls[1]
    assert idx2 == "sentry"
    assert q2 == {"query": {"ids": {"values": ["a", "b"]}}}


def test_cleanup_deletes_old_indices(es):
    storage = OpenSearchNodeStorage(es=es, alias_name="sentry")

    es.indices.alias_map["sentry"] = {
        "sentry-2022-12-29-fixed",
        "sentry-2022-12-30",
        "sentry-2023-01-01",
    }

    cutoff = datetime(2022, 12, 31, tzinfo=timezone.utc)
    storage.cleanup(cutoff)

    assert "sentry-2022-12-29-fixed" in es.indices.deleted_indices
    assert "sentry-2022-12-30" in es.indices.deleted_indices
    assert "sentry-2023-01-01" not in es.indices.deleted_indices
