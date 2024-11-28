import sys
from collections import defaultdict
from datetime import UTC, datetime
from io import StringIO

import smart_open
from pydantic import BaseModel
from typing_extensions import Annotated

from harmonia.base.validators import NAME, SCHEME, VERSION, makedirs


class LogProvider:
    def __init__(self, uri: str = "-", handle_stdout: bool = False):
        self.handle = None
        if uri == "-":
            self.handle = sys.stdout
            return

        if "://" not in uri:
            raise ValueError("URI must contain a protocol")
        makedirs(uri)
        self.handle = smart_open.open(uri, "w")
        # this can be dangerous, only used by the log factory
        if handle_stdout:
            sys.stdout = self.handle
            sys.stderr = self.handle

    def msg(self, msg: str):
        self.handle.write(f"{datetime.now(UTC)} | {msg}\n")

    def close(self):
        if self.handle is not None:
            self.handle.close()

    def __del__(self):
        self.close()


class LogProviderFactory(BaseModel, frozen=True):
    uri: Annotated[str, NAME, VERSION, SCHEME]

    def build(self, version: str, name: str) -> LogProvider:
        return LogProvider(
            self.uri.format(version=version, name=name),
            handle_stdout=True,
        )


class TestLogProvider(LogProvider):
    def __init__(self):
        self.handle = StringIO()


PRINT_LOGGER = LogProvider()
TEST_LOGGER = TestLogProvider()


class MetricProvider:
    def __init__(self, uri: str = "-", log_provider: LogProvider | None = None):
        self.log_provider = log_provider
        self._params = {}
        self._metrics = defaultdict(list)
        # allow for @property implementations
        self.params = self._params
        self.metrics = self._metrics
        self.handle = None
        if uri == "-":
            return

        if "://" not in uri:
            raise ValueError("URI must contain a protocol")
        makedirs(uri)
        self.handle = smart_open.open(uri, "w")

    def close(self):
        if self.handle is None:
            return

        for param, value in self._params.items():
            self.handle.write(f"{param}: {value}\n")
        for metric, values in self._metrics.items():
            self.handle.write(f"{metric}: ")
            self.handle.write(",".join(f"{v:.4f}" for v in values))
            self.handle.write("\n")
        self.handle.close()

    def __del__(self):
        self.close()
        if self.log_provider is not None:
            self.log_provider.close()

    def log_param(self, param: str, value: str):
        self._params[param] = value
        if self.log_provider is not None:
            self.log_provider.msg(f"param: {param} = {value}")

    def get_param(self, param: str) -> str:
        return self._params[param]

    def log_metric(self, metric: str, value: float):
        self._metrics[metric].append(value)
        if self.log_provider is not None:
            self.log_provider.msg(f"metric: {metric} = {value:.4f}")

    def get_metric(self, metric: str) -> list[float]:
        return self._metrics[metric]


class MetricFactory(BaseModel, frozen=True):
    uri: Annotated[str, NAME, VERSION, SCHEME]

    def build(
        self,
        version: str,
        name: str,
        log_provider: LogProvider | None = None,
    ) -> MetricProvider:
        return MetricProvider(
            self.uri.format(version=version, name=name),
            log_provider=log_provider,
        )


TEST_METRIC = MetricProvider(log_provider=TEST_LOGGER)


# Some defaults for a local run
DEFAULT_LOG_FACTORY = LogProviderFactory(uri="file://./logs/{version}/{name}.log")
DEFAULT_METRIC_FACTORY = MetricFactory(uri="file://./logs/{version}/{name}.metrics")
