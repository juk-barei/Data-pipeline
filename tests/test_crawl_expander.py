from unittest.mock import Mock

from pipeline.config import Settings
from pipeline.crawl.expander import ExpanderWorker


def test_constructor_retains_dependencies_without_io():
    store = Mock()
    settings = Settings(hop_depth=3, batch_size=7)
    worker = ExpanderWorker(store, settings)
    assert worker.store is store
    assert worker.settings is settings
    assert store.mock_calls == []
