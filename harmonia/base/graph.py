import os
import subprocess
from collections import defaultdict
from typing import Annotated, Any, Self

from pydantic import BaseModel, model_validator
from pydantic.functional_validators import BeforeValidator

from harmonia.base import log
from harmonia.base.validators import FILE_SCHEME, SCHEME, UNIQUE_ELEMENTS, VERSION

HEARTBEAT_TIMEOUT = 0.1


class NodeMetadata:
    def __init__(self, logger: log.LogProvider, meta: Any):
        self.logger = logger
        self.meta = meta


class Node(BaseModel, frozen=True):
    name: str
    cmd: tuple[str, ...]
    log_provider_factory: log.LogProviderFactory

    def __lt__(self, other):
        return self.name < other.name

    def __str__(self: Self) -> str:
        return f"{self.name} {self.cmd}"

    def __repr__(self: Self) -> str:
        return f"Node<{self.name} {self.cmd}>"

    def run(self: Self, version: str, args: list[str]) -> NodeMetadata:
        args = list(self.cmd) + args
        logger = self.log_provider_factory.build(version, self.name)
        process = subprocess.Popen(
            args,
            stdout=logger.handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return NodeMetadata(logger, process)

    def heartbeat(self: Self, nm: NodeMetadata, version: str) -> int | None:
        try:
            return nm.meta.wait(HEARTBEAT_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass
        return None


class Edge(BaseModel, frozen=True):
    uri: Annotated[str, SCHEME]

    def __eq__(self, other):
        return self.uri == other.uri

    def __lt__(self, other):
        return self.uri < other.uri

    def __str__(self):
        return f"{self.uri}"

    def __repr__(self):
        return f"Edge<{self.uri}>"

    def build_uri(self, version: str) -> str:
        return self.uri.format(version=version)

    def exists(self) -> bool:
        return True


class LocalEdge(Edge):
    uri: Annotated[str, VERSION, FILE_SCHEME]

    def exists(self) -> bool:
        return os.path.exists(self.uri[len("file://") :])


def make_immutable_dict(
    mapping: dict[str, str | Edge] | tuple[tuple[str, str | Edge], ...],
) -> tuple[tuple[str, str | Edge], ...]:
    if hasattr(mapping, "items"):
        return tuple(mapping.items())
    return mapping


IMMUTABLE_DICT = BeforeValidator(make_immutable_dict)


class Process(BaseModel, frozen=True):
    node: Node
    flags: tuple[str, ...] = ()
    options: Annotated[tuple[tuple[str, str | Edge], ...], IMMUTABLE_DICT] = ()
    input_edges: tuple[Edge, ...] = ()
    output_edges: tuple[Edge, ...] = ()
    strip_scheme: bool = False

    @model_validator(mode="after")
    def validate(self) -> Self:
        assert len(self.output_edges) > 0, "Process has no outputs"
        return self

    def __lt__(self, other):
        return self.node < other.node


class CompiledGraph(BaseModel, frozen=True):
    name: str
    order: Annotated[tuple[Process, ...], UNIQUE_ELEMENTS]
    input_edges: Annotated[tuple[Edge, ...], UNIQUE_ELEMENTS]

    def disjoint(self, initial_edge: Edge) -> bool:
        edges = set()
        processes = set()
        return edges == processes

    def run(self, version: str):
        # TODO full rework
        for processes in self.order:
            for process in self.processes:
                process.node.run(
                    version,
                    [
                        e.build_uri(version)
                        for e in process.input_edges + process.output_edges
                    ],
                )


class Graph(BaseModel, frozen=True):
    processes: Annotated[tuple[Process, ...], UNIQUE_ELEMENTS]
    edges: Annotated[tuple[Edge, ...], UNIQUE_ELEMENTS]

    @model_validator(mode="after")
    def validate(self) -> Self:
        io_edges = []
        output_edges = defaultdict(list)
        for process in self.processes:
            edges = (
                list(process.input_edges)
                + list(process.output_edges)
                + [e for e in dict(process.options).values() if isinstance(e, Edge)]
            )
            for edge in edges:
                assert edge in self.edges, f"Edge {edge} not found in graph"
            io_edges.extend(edges)

            for edge in process.output_edges:
                output_edges[edge].append(process)

        for edge in self.edges:
            assert edge in io_edges, f"Edge {edge} not attached to a process"
        for edge, processes in output_edges.items():
            assert (
                len(processes) == 1
            ), f"Output Edge {edge} attached to multiple processes: {processes}"
        self.full_io()
        return self

    def full_io(self) -> tuple[list[Edge], list[Edge], list[Edge]]:
        process_inputs = []
        process_outputs = []
        for process in self.processes:
            process_inputs.extend(process.input_edges)
            process_outputs.extend(process.output_edges)

        inputs = [e for e in self.edges if e not in process_outputs]
        middle = [e for e in self.edges if e in process_inputs and e in process_outputs]
        outputs = [e for e in self.edges if e not in process_inputs]
        assert inputs, "Graph has no inputs"
        assert outputs, "Graph has no outputs"

        return inputs, middle, outputs

    def compile_graph(
        self,
        name: str,
        inputs: list[Edge],
        middle: list[Edge],
        outputs: list[Edge],
    ) -> tuple[CompiledGraph, list[Edge]]:
        order = []
        output_edges = {}  # precompile mapping - we already validated it
        for process in self.processes:
            for edge in process.output_edges:
                output_edges[edge] = process

        cur_inputs = set(inputs)
        cur_middle = set(middle)
        cur_outputs = set(outputs)
        assert cur_inputs & cur_middle & cur_outputs == set(), "Edges are not unique"
        while cur_outputs:
            new_outputs = set()
            for edge in cur_outputs:
                to_run = output_edges[edge]
                if to_run in order:
                    # already accountde for due to another output edge
                    continue
                order.append(to_run)
                required = set(to_run.input_edges) & cur_middle
                new_outputs |= required
            cur_outputs = new_outputs
        return CompiledGraph(name=name, order=order, input_edges=inputs)
