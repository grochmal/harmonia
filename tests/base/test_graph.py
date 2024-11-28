import json
import sys
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from harmonia.base import graph, validators


def test_node_creation_validates(log_provider_factory):
    graph.Node(name="waltz", cmd=["ls"], log_provider_factory=log_provider_factory)

    with pytest.raises(ValidationError):
        graph.Node(name="waltz", cmd="ls", log_provider_factory=None)


def test_node_can_be_serialised(log_provider_factory):
    node = graph.Node(
        name="waltz",
        cmd=["ls"],
        log_provider_factory=log_provider_factory,
    )

    serialised = json.dumps(node.model_dump())
    reconstructed = graph.Node.model_validate(json.loads(serialised))
    assert node == reconstructed
    assert str(node) == str(reconstructed)
    assert repr(node) == repr(reconstructed)


def test_running_node_gives_heatbeats(log_provider_factory):
    node = graph.Node(
        name="waltz",
        cmd=[sys.executable, "-c", "import time; time.sleep(0.1)"],
        log_provider_factory=log_provider_factory,
    )

    metadata = node.run("allegro_finito", [])
    return_code = node.heartbeat(metadata, "allegro_finito")
    assert return_code is None

    time.sleep(0.2)
    return_code = node.heartbeat(metadata, "allegro_finito")
    assert return_code == 0


def test_edge_creation_validates():
    graph.Edge(uri="file://./data/score.tar.gz")

    with pytest.raises(ValidationError):
        graph.Edge(uri="data/score.tar.gz")


def test_edge_can_be_serialised():
    edge = graph.Edge(uri="file://./data/composition.tar.gz")

    serialised = json.dumps(edge.model_dump())
    reconstructed = graph.Edge.model_validate(json.loads(serialised))
    assert edge == reconstructed
    assert str(edge) == str(reconstructed)
    assert repr(edge) == repr(reconstructed)


def test_edge_vs_local_exists(tmp_path: Path):
    music_score = f"{tmp_path}/data/{{version}}/composition.tar.gz"
    music_score_uri = f"file://{music_score}"
    validators.makedirs(music_score_uri)
    edge = graph.Edge(uri=music_score_uri)
    local_edge = graph.LocalEdge(uri=music_score_uri)

    assert edge.build_uri("allegro") == local_edge.build_uri("allegro")
    assert edge.exists() is True
    assert local_edge.exists() is False

    with open(music_score, "w") as f:
        f.write("music")

    assert local_edge.exists() is True


def test_process_creation_validates(log_provider_factory):
    node = graph.Node(
        name="serenade",
        cmd=["ls"],
        log_provider_factory=log_provider_factory,
    )
    guitar = graph.Edge(uri="file://./data/guitar.parquet")
    percussion = graph.Edge(uri="file://./data/percussion.parquet")
    song = graph.LocalEdge(uri="file://./data/{version}/lyrics.parquet")
    love = graph.LocalEdge(uri="file://./data/{version}/love.parquet")
    graph.Process(
        node=node,
        flags=["--verbose"],
        options={"--song": song, "--lyrics": "full"},
        input_edges=[guitar, percussion],
        output_edges=[love],
    )

    with pytest.raises(ValidationError):
        graph.Process(
            node=node,
            flags=None,
            options={},
            input_edges=[guitar],
            output_edges=[love],
        )

    with pytest.raises(ValidationError):
        graph.Process(
            node=node,
            input_edges=[guitar],
            output_edges=[],
        )


def test_process_can_be_serialised(log_provider_factory):
    node = graph.Node(
        name="rock and roll",
        cmd=["ls"],
        log_provider_factory=log_provider_factory,
    )
    guitar = graph.Edge(uri="file://./data/guitar.parquet")
    song = graph.LocalEdge(uri="file://./data/{version}/lyrics.parquet")
    love = graph.LocalEdge(uri="file://./data/{version}/love.parquet")
    process = graph.Process(
        node=node,
        flags=["--verbose"],
        options={"--song": song, "--lyrics": "piecemeal"},
        input_edges=[guitar],
        output_edges=[love],
    )

    serialised = json.dumps(process.model_dump())
    reconstructed = graph.Process.model_validate(json.loads(serialised))
    assert process == reconstructed
    assert str(process) == str(reconstructed)
    assert repr(process) == repr(reconstructed)


def test_ensure_graph_consistency():
    pass
