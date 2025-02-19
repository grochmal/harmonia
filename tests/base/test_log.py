import os
from pathlib import Path

import pytest

from harmonia.base import log


def test_local_makedirs(tmp_path: Path):
    log.makedirs(f"file://{tmp_path}/violin/contrabass/triangle/strings.txt")
    assert (tmp_path / "violin/contrabass/triangle").exists()


def test_local_single_file_makedirs(tmp_path: Path):
    os.chdir(tmp_path)
    log.makedirs("file://strings.txt")
    assert not (tmp_path / "strings.txt").exists()


def test_remote_makedirs(tmp_path: Path):
    log.makedirs(f"http://{tmp_path}/violin/contrabass/triangle")
    assert not (tmp_path / "violin/contrabass/triangle").exists()


def test_log_msg():
    log.PRINT_LOGGER.msg("msg on dogs")


def test_test_logger():
    log.TEST_LOGGER.msg("info on cats")
    log.TEST_LOGGER.msg("msg on dogs")

    assert "info on cats\n" in log.TEST_LOGGER.handle.getvalue()
    assert "msg on dogs\n" in log.TEST_LOGGER.handle.getvalue()


def test_log_provider_bad_uri():
    with pytest.raises(ValueError):
        log.LogProvider(uri="bad_pitch")


def test_log_to_file(tmp_path: Path):
    log_file = tmp_path / "chorale.log"

    log_provider = log.LogProvider(uri=f"file://{log_file}")
    log_provider.msg("da capo aria")
    log_provider.msg("basso continuo")
    log_provider.close()

    with open(log_file) as f:
        value = f.read()

    assert "da capo aria\n" in value
    assert "basso continuo\n" in value


def test_log_provider_factory(tmp_path: Path):
    factory = log.LogProviderFactory(
        uri=f"file://{tmp_path}/logs/{{version}}/{{name}}.log"
    )
    logger = factory.build("loud_aria", "flute_player")
    logger.msg("four seasons")
    logger.msg("vivaldi")
    logger.close()

    with open(tmp_path / "logs/loud_aria/flute_player.log") as f:
        value = f.read()
    assert "four seasons\n" in value
    assert "vivaldi\n" in value


def test_log_param():
    log.TEST_METRIC.log_param("momentum", "adaptive")
    log.TEST_METRIC.log_param("learning_rate", "0.01")

    assert log.TEST_METRIC.get_param("momentum") == "adaptive"
    assert log.TEST_METRIC.get_param("learning_rate") == "0.01"


def test_log_metric():
    log.TEST_METRIC.log_metric("loss", 0.1)
    log.TEST_METRIC.log_metric("loss", 0.2)
    log.TEST_METRIC.log_metric("f1_score", 0.9)
    log.TEST_METRIC.log_metric("f1_score", 0.8)

    assert log.TEST_METRIC.get_metric("loss") == [0.1, 0.2]
    assert log.TEST_METRIC.get_metric("f1_score") == [0.9, 0.8]


def test_metric_provider(tmp_path: Path):
    metrics_file = tmp_path / "{version}/{name}.metrics"
    factory = log.MetricFactory(uri=f"file://{metrics_file}")
    metric_provider = factory.build("piacere", "tempo")
    metric_provider.log_param("momentum", "adaptive")
    metric_provider.log_metric("loss", 0.1)
    metric_provider.log_metric("loss", 0.07)
    metric_provider.log_metric("lr", 0.01)
    metric_provider.log_metric("lr", 0.05)

    assert metric_provider.get_metric("loss") == [0.1, 0.07]
    assert metric_provider.get_metric("lr") == [0.01, 0.05]

    del metric_provider
    with open(str(metrics_file).format(version="piacere", name="tempo")) as f:
        value = f.read()

    assert "momentum: adaptive\n" in value
    assert "loss: 0.1000,0.0700\n" in value
    assert "lr: 0.0100,0.0500\n" in value


def test_log_metric_through_logger(tmp_path: Path):
    log_file = tmp_path / "contralto.log"

    log_provider = log.LogProvider(uri=f"file://{log_file}")
    metric_provider = log.DEFAULT_METRIC_FACTORY.build("major", "canto", log_provider)
    metric_provider.log_param("tempo", "adagio")
    metric_provider.log_metric("pitch", 0.1)
    metric_provider.log_metric("pitch", 0.2)
    del metric_provider

    with open(log_file) as f:
        value = f.read()

    assert "param: tempo = adagio\n" in value
    assert "metric: pitch = 0.1000\n" in value
    assert "metric: pitch = 0.2000\n" in value


def test_metric_provider_bad_uri():
    with pytest.raises(ValueError):
        log.MetricProvider(uri="bad_tone")
