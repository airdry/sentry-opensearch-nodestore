# tests/conftest.py
import sys
import types
import os
import pytest

# Stub 'sentry.nodestore.base.NodeStorage'
if "sentry" not in sys.modules:
    sentry_mod = types.ModuleType("sentry")
    sys.modules["sentry"] = sentry_mod

if "sentry.nodestore" not in sys.modules:
    nodestore_mod = types.ModuleType("sentry.nodestore")
    sys.modules["sentry.nodestore"] = nodestore_mod

if "sentry.nodestore.base" not in sys.modules:
    base_mod = types.ModuleType("sentry.nodestore.base")

    class NodeStorage:
        def __init__(self, *args, **kwargs):
            pass

    base_mod.NodeStorage = NodeStorage
    sys.modules["sentry.nodestore.base"] = base_mod

# Link nested modules
sys.modules["sentry"].nodestore = sys.modules["sentry.nodestore"]
sys.modules["sentry.nodestore"].base = sys.modules["sentry.nodestore.base"]

# Optional: stub 'opensearchpy.exceptions' if not installed
try:
    import opensearchpy  # noqa: F401
except Exception:
    opensearchpy_mod = types.ModuleType("opensearchpy")
    exceptions_mod = types.ModuleType("opensearchpy.exceptions")

    class NotFoundError(Exception):
        def __init__(self, *args, **kwargs):
            super().__init__(*args)

    class ConflictError(Exception):
        def __init__(self, *args, **kwargs):
            super().__init__(*args)

    exceptions_mod.NotFoundError = NotFoundError
    exceptions_mod.ConflictError = ConflictError

    opensearchpy_mod.exceptions = exceptions_mod
    sys.modules["opensearchpy"] = opensearchpy_mod
    sys.modules["opensearchpy.exceptions"] = exceptions_mod


def pytest_configure(config):
    # Register the custom marker so PyTest doesn't warn
    config.addinivalue_line(
        "markers",
        "integration: marks tests that talk to real services (enable with RUN_INTEGRATION_TESTS=1)",
    )

def pytest_collection_modifyitems(config, items):
    # Only run integration tests when the env var is set
    run_integration = os.getenv("RUN_INTEGRATION_TESTS", "").lower() in ("1", "true", "yes", "on")
    if run_integration:
        return

    skip_integration = pytest.mark.skip(
        reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=1 to run."
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)