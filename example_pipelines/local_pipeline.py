from harmonia.base.graph import Edge, Graph, LocalEdge, Node, Process
from harmonia.base.log import LogProviderFactory

LOG_PROVIDER_FACTORY = LogProviderFactory(
    uri="file://./runtime/{version}/logs/{name}.log"
)

PUBMED_INPUT = Edge(
    uri="file://./data/oa_comm_txt.PMC000xxxxxx.baseline.2024-06-17.tar.gz"
)
PUBMED_CACHE = LocalEdge(uri="file://./runtime/{version}/cache")
TOKENIZED = LocalEdge(uri="file://./runtime/{version}/tokenized")
NER_DICTIONARIES = LocalEdge(uri="file://./runtime/{version}/ner_dictionaries")
NER_RESULTS = LocalEdge(uri="file://./runtime/{version}/ner_results")


UNZIP_PUBMED = Process(
    node=Node(
        name="unzip_pubmed",
        cmd=["tar", "-xzf"],
        log_provider_factory=LOG_PROVIDER_FACTORY,
    ),
    input_edges=[PUBMED_INPUT],
    output_edges=[PUBMED_CACHE],
)
TOKENIZE_DOCS = Process(
    node=Node(
        name="tokenize_docs",
        cmd=["python", "./example_pipelines/scripts/tokenize_docs.py"],
        log_provider_factory=LOG_PROVIDER_FACTORY,
    ),
    input_edges=[PUBMED_CACHE],
    output_edges=[TOKENIZED],
)
BUILD_NER_DICTS = Process(
    node=Node(
        name="bild_ner_dicts",
        cmd=["python", "./example_pipelines/scripts/build_ner_dicts.py"],
        log_provider_factory=LOG_PROVIDER_FACTORY,
    ),
    input_edges=[TOKENIZED],
    output_edges=[NER_DICTIONARIES],
)
DO_NER = Process(
    node=Node(
        name="ner",
        cmd=["python", "./example_pipelines/scripts/build_ner_dicts.py"],
        log_provider_factory=LOG_PROVIDER_FACTORY,
    ),
    input_edges=[TOKENIZED, NER_DICTIONARIES],
    output_edges=[NER_RESULTS],
)

GRAPH = Graph(
    processes=[UNZIP_PUBMED, TOKENIZE_DOCS, BUILD_NER_DICTS, DO_NER],
    edges=[PUBMED_INPUT, PUBMED_CACHE, TOKENIZED, NER_DICTIONARIES, NER_RESULTS],
)
