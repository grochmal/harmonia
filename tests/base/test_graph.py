import json
import sys
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from harmonia.base import graph, log, validators


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


def test_edges_can_be_compared():
    raw_edge = graph.Edge(uri="file://./data/input/notes.tar.gz")
    versioned_edge = graph.Edge(uri="file://./data/{version}/performance.parquet")
    local_edge = graph.LocalEdge(uri="file://./data/{version}/andate.parquet")
    performance = graph.LocalEdge(uri="file://./data/{version}/performance.parquet")
    edges = [raw_edge, versioned_edge, local_edge, performance]

    assert raw_edge in edges
    assert local_edge in edges
    assert versioned_edge == performance
    assert performance != local_edge


def test_process_creation_validates(log_provider_factory: log.LogProviderFactory):
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


def test_process_can_be_serialised(log_provider_factory: log.LogProviderFactory):
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


def test_graph_creation_validates(log_provider_factory: log.LogProviderFactory):
    guitar = graph.Edge(uri="file://./data/guitar.parquet")
    song = graph.LocalEdge(uri="file://./data/{version}/lyrics.parquet")
    love = graph.LocalEdge(uri="file://./data/{version}/love.parquet")
    reggae = graph.Process(
        node=graph.Node(
            name="reggae",
            cmd=["ls"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[guitar],
        output_edges=[song],
    )
    metal = graph.Process(
        node=graph.Node(
            name="metal",
            cmd=["ls"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[song],
        output_edges=[love],
    )
    graph.Graph(
        name="rock and roll",
        processes=[reggae, metal],
        edges=[guitar, song, love],
    )

    with pytest.raises(ValidationError):
        graph.Graph(
            name="rock and roll",
            processes=[reggae, metal],
            edges=[guitar, song],
        )

    with pytest.raises(ValidationError):
        graph.Graph(
            name="rock and roll",
            processes=[reggae],
            edges=[guitar, song, love],
        )


def test_disjoint_graph_is_invalid(log_provider_factory: log.LogProviderFactory):
    flute = graph.Edge(uri="file://./data/flute.parquet")
    lyrics = graph.LocalEdge(uri="file://./data/{version}/lyrics.parquet")
    guitar = graph.Edge(uri="file://./data/guitar.parquet")
    song = graph.LocalEdge(uri="file://./data/{version}/song.parquet")
    reggae = graph.Process(
        node=graph.Node(
            name="reggae",
            cmd=["ls"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[flute],
        output_edges=[lyrics],
    )
    metal = graph.Process(
        node=graph.Node(
            name="metal",
            cmd=["ls"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[guitar],
        output_edges=[song],
    )
    with pytest.raises(ValidationError):
        graph.Graph(
            name="rock and roll",
            processes=[reggae, metal],
            edges=[flute, guitar, lyrics, song],
        )


def test_graph_can_serialised(swan_lake_graph: graph.Graph):
    serialised = json.dumps(swan_lake_graph.model_dump())
    reconstructed = graph.Graph.model_validate(json.loads(serialised))
    assert swan_lake_graph == reconstructed


def test_full_io_return_unique_edges(swan_lake_graph: graph.Graph):
    inputs, middle, outputs = swan_lake_graph.full_io()
    assert len(inputs) == 1
    assert len(middle) == len(swan_lake_graph.edges) - 3
    assert len(outputs) == 2
    for i in inputs:
        assert i not in middle
        assert i not in outputs
    for i in middle:
        assert i not in inputs
        assert i not in outputs
    for i in outputs:
        assert i not in inputs
        assert i not in middle


def test_compile_full_graph(swan_lake_graph: graph.Graph):
    inputs, middle, outputs = swan_lake_graph.full_io()
    compiled = swan_lake_graph.compile_graph("swan_lake", inputs, middle, outputs)
    assert compiled.input_edges == tuple(inputs)


def test_compile_single_process(swan_lake_graph: graph.Graph):
    inputs, middle, outputs = swan_lake_graph.full_io()

    e_allegro_guisto = graph.Edge(
        uri="file://./data/swan-lake/{version}/allegro-guisto/"
    )
    e_tempo_di_valse = graph.Edge(
        uri="file://./data/swan-lake/{version}/tempo-di-valse/"
    )
    middle = set(middle)
    middle -= set([e_allegro_guisto, e_tempo_di_valse])
    middle |= set(inputs)
    middle |= set(outputs)
    compiled = swan_lake_graph.compile_graph(
        "waltz-no-2",
        [e_allegro_guisto],
        sorted(middle),
        [e_tempo_di_valse],
    )

    assert len(compiled.order) == 1
    process = compiled.order[0]
    assert process.node.name == "waltz-no-2"
    assert process.input_edges == (e_allegro_guisto,)
    assert process.output_edges == (e_tempo_di_valse,)


def test_compile_big_subgraph(
    log_provider_factory: log.LogProviderFactory,
    swan_lake_graph: graph.Graph,
):
    e_allegro_guisto = graph.Edge(
        uri="file://./data/swan-lake/{version}/allegro-guisto/"
    )
    e_tempo_di_valse = graph.Edge(
        uri="file://./data/swan-lake/{version}/tempo-di-valse/"
    )
    e_allegro_moderato = graph.Edge(
        uri="file://./data/swan-lake/{version}/allegro-moderato/"
    )
    e_entree = graph.Edge(uri="file://./data/swan-lake/{version}/entree/")
    e_intrada = graph.Edge(uri="file://./data/swan-lake/{version}/intrada/")
    e_allegro_sostenuto = graph.Edge(
        uri="file://./data/swan-lake/{version}/allegro-sostenuto/"
    )
    e_andante_allegro = graph.Edge(
        uri="file://./data/swan-lake/{version}/andante-allegro/"
    )
    e_tempo_di_valse_non_troppo_vivo = graph.Edge(
        uri="file://./data/swan-lake/{version}/tempo-di-valse-non-troppo-vivo/"
    )
    e_coda_allegro_molto_vivace = graph.Edge(
        uri="file://./data/swan-lake/{version}/coda-allegro-molto-vivace/"
    )
    e_pass_d_action = graph.Edge(uri="file://./data/swan-lake/{version}/pass-d-action/")
    p_waltz_no_2 = graph.Process(
        node=graph.Node(
            name="waltz-no-2",
            cmd=[sys.executable, "-c", "print('Waltz No.2')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_allegro_guisto],
        output_edges=[e_tempo_di_valse],
    )
    p_scene_pas_de_trois = graph.Process(
        node=graph.Node(
            name="scene-pas-de-trois",
            cmd=[sys.executable, "-c", "print('Scene Pas de Trois')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_tempo_di_valse],
        output_edges=[e_entree, e_intrada, e_allegro_sostenuto],
    )
    p_andante_sostenuto = graph.Process(
        node=graph.Node(
            name="andante-sostenuto",
            cmd=[sys.executable, "-c", "print('Andante Sostenuto')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_entree],
        output_edges=[e_andante_allegro],
    )
    p_allegro_no_4 = graph.Process(
        node=graph.Node(
            name="allegro-no-4",
            cmd=[sys.executable, "-c", "print('Allegro No.4')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_intrada],
        output_edges=[e_tempo_di_valse_non_troppo_vivo],
    )
    p_presto = graph.Process(
        node=graph.Node(
            name="presto",
            cmd=[sys.executable, "-c", "print('Presto')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_allegro_sostenuto, e_allegro_moderato],
        output_edges=[e_coda_allegro_molto_vivace],
    )
    p_pass_de_deux = graph.Process(
        node=graph.Node(
            name="pass-de-deux",
            cmd=[sys.executable, "-c", "print('Pass de Deux')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_tempo_di_valse_non_troppo_vivo, e_coda_allegro_molto_vivace],
        output_edges=[e_pass_d_action],
    )
    g = graph.Graph(
        name="swan-lake",
        processes=[
            p_waltz_no_2,
            p_scene_pas_de_trois,
            p_andante_sostenuto,
            p_allegro_no_4,
            p_presto,
            p_pass_de_deux,
        ],
        edges=[
            e_allegro_guisto,
            e_tempo_di_valse,
            e_allegro_moderato,
            e_entree,
            e_intrada,
            e_allegro_sostenuto,
            e_andante_allegro,
            e_tempo_di_valse_non_troppo_vivo,
            e_coda_allegro_molto_vivace,
            e_pass_d_action,
        ],
    )

    inputs, middle, outputs = swan_lake_graph.full_io()
    middle = set(middle)
    middle -= set(
        [
            e_allegro_guisto,
            e_allegro_moderato,
            e_andante_allegro,
            e_pass_d_action,
        ]
    )
    middle |= set(inputs)
    middle |= set(outputs)
    compiled = swan_lake_graph.compile_graph(
        "subgraph",
        [e_allegro_guisto, e_allegro_moderato],
        sorted(middle),
        [e_andante_allegro, e_pass_d_action],
    )

    assert compiled == g.compile_graph("subgraph", *g.full_io())
