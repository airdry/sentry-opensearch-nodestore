import sys
from unittest.mock import MagicMock

# --- The Definitive Solution: Manually Mocking a Non-Existent Module ---

# 1. Create a mock object that will act as the top-level 'sentry' module.
sentry_mock = MagicMock()

# 2. Define a simple stub class. This is what our backend will inherit from.
class MockNodeStorage:
    """A mock stand-in for Sentry's NodeStorage base class."""
    def __init__(self, **kwargs):
        pass

# 3. Set the nested structure that `backend.py` expects.
#    When `from sentry.nodestore.base import NodeStorage` is called, Python will
#    look at our `sentry_mock`, find `nodestore`, then `base`, then `NodeStorage`.
sentry_mock.nodestore.base.NodeStorage = MockNodeStorage

# 4. The crucial step: Inject the mock objects into Python's module cache.
#    Any code that now tries to `import sentry` or `from sentry...` will
#    find our mock objects here and will not attempt to find the real package
#    on the filesystem, thus preventing the ModuleNotFoundError.
sys.modules['sentry'] = sentry_mock
sys.modules['sentry.nodestore'] = sentry_mock.nodestore
sys.modules['sentry.nodestore.base'] = sentry_mock.nodestore.base

