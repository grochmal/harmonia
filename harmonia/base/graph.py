import os
import subprocess
import traceback

from pydantic import BaseModel, model_validator
from typing_extensions import Annotated, Self

from harmonia.base import log
from harmonia.base.validators import FILE_SCHEME, SCHEME, VERSION


class Node(BaseModel):
    name: str
    cmd: list[str]
    log_provider_factory: log.LogProviderFactory

    def __str__(self):
        return f"{self.name} {self.cmd}"

    def __repr__(self):
        return f"Node<{self.name} {self.cmd}>"

    def run(self, version: str, args: list[str]):
        args = self.cmd + args
        logger = self.log_provider_factory.build(version, self.name)
        process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in process.stdout:
            logger.msg(line)
        try:
            process.check_returncode()
        except subprocess.CalledProcessError as e:
            logger.msg(f"Error running command: {e}")
            logger.msg(traceback.format_exc())


class Edge(BaseModel):
    uri: Annotated[str, SCHEME]

    def __eq__(self, other):
        return self.uri == other.uri

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
        return os.path.exists(self._uri[len("file://") :])


class Process(BaseModel):
    node: Node
    flags: list[str] = []
    options: dict[str, str | Edge] = []
    input_edges: list[Edge] = []
    output_edges: list[Edge] = []


class CompiledGraph(BaseModel):
    order: list[set[Process]]
    input_edges: set[Edge]

    def run(self, version: str):
        for processes in self.order:
            for process in self.processes:
                process.node.run(
                    version,
                    [
                        e.build_uri(version)
                        for e in process.input_edges + process.output_edges
                    ],
                )


class Graph(BaseModel):
    processes: set[Node]
    edges: set[Edge]

    @model_validator(mode="after")
    def validate(self) -> Self:
        for process in self.processes:
            edges = (
                process.input_edges
                + process.output_edges
                + [e for e in process.options.values() if isinstance(e, Edge)],
            )
            for edge in edges:
                assert edge in self.edges, f"Edge {edge} not found in graph"
            for edge in self.edges:
                assert edge in edges, f"Edge {edge} not attached to a process"
        self.full_io()
        return self

    def full_io(self) -> tuple(list[Edge], list[Edge], list[Edge]):
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
        inputs: list[Edge],
        middle: list[Edge],
        outputs: list[Edge],
    ) -> tuple(CompiledGraph, list[Edge]):
        order = []
        processes = set(self.processes)
        cur_inputs = set(inputs)
        cur_middle = set(middle)
        cur_outputs = set(outputs)
        while processes:
            step = []
            new_inputs = []
            for process in processes:
                if all([edge in inputs for edge in process.input_edges]):
                    step.append(process)
                    new_inputs.extend(process.output_edges)
            if len(step) == 0:
                # either something went terribly wrong
                # or we got a disjoint graph,
                # let the caller deal with this
                break
            step = set(step)
            new_inputs = set(new_inputs)
            processes -= step
            cur_inputs += new_inputs
            cur_middle -= new_inputs
            cur_outputs -= new_inputs
            order.append(step)
        return CompiledGraph(order=order, input_edges=inputs), cur_outputs
