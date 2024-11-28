from pathlib import Path

import pytest

from harmonia.base import log


@pytest.fixture
def log_provider_factory(tmp_path: Path) -> log.LogProviderFactory:
    return log.LogProviderFactory(
        uri=f"file://{tmp_path}/logs/{{version}}/{{name}}.log"
    )
