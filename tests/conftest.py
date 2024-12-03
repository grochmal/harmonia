import sys
from pathlib import Path

import pytest

from harmonia.base import graph, log


@pytest.fixture
def log_provider_factory(tmp_path: Path) -> log.LogProviderFactory:
    return log.LogProviderFactory(
        uri=f"file://{tmp_path}/logs/{{version}}/{{name}}.log"
    )


@pytest.fixture
def swan_lake_graph(log_provider_factory: log.LogProvider) -> graph.Graph:
    e_act_1 = graph.Edge(uri="file://./data/act-1/score/")
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
    e_tempo_di_polaca = graph.Edge(
        uri="file://./data/swan-lake/{version}/tempo-di-polaca/"
    )
    e_sujet_andante_finale = graph.Edge(
        uri="file://./data/swan-lake/{version}/sujet-andante-finale/"
    )

    p_scene_no_1 = graph.Process(
        node=graph.Node(
            name="scene-no-1",
            cmd=[sys.executable, "-c", "print('Scene No.1')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_act_1],
        output_edges=[e_allegro_guisto],
    )
    p_waltz_no_2 = graph.Process(
        node=graph.Node(
            name="waltz-no-2",
            cmd=[sys.executable, "-c", "print('Waltz No.2')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_allegro_guisto],
        output_edges=[e_tempo_di_valse],
    )
    p_scene_no_3 = graph.Process(
        node=graph.Node(
            name="scene-no-3",
            cmd=[sys.executable, "-c", "print('Scene No.3')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_allegro_guisto],
        output_edges=[e_allegro_moderato],
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
    p_sujet_no_7 = graph.Process(
        node=graph.Node(
            name="sujet-no-7",
            cmd=[sys.executable, "-c", "print('Sujet No.7')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_pass_d_action, e_allegro_moderato],
        output_edges=[e_tempo_di_polaca],
    )
    p_dance_with_goblets = graph.Process(
        node=graph.Node(
            name="dance-with-goblets",
            cmd=[sys.executable, "-c", "print('Dance with Goblets')"],
            log_provider_factory=log_provider_factory,
        ),
        input_edges=[e_tempo_di_polaca],
        output_edges=[e_sujet_andante_finale],
    )
    return graph.Graph(
        name="swan-lake",
        processes=[
            p_scene_no_1,
            p_waltz_no_2,
            p_scene_no_3,
            p_scene_pas_de_trois,
            p_andante_sostenuto,
            p_allegro_no_4,
            p_presto,
            p_pass_de_deux,
            p_sujet_no_7,
            p_dance_with_goblets,
        ],
        edges=[
            e_act_1,
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
            e_tempo_di_polaca,
            e_sujet_andante_finale,
        ],
    )
