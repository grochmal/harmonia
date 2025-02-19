import json
import os
import posixpath
from typing import Annotated

from pydantic import BaseModel, ValidationError

from harmonia.base import graph
from harmonia.base.validators import SCHEME


class IncompatibleGraph(Exception):
    value: str


class UnreadableGraph(Exception):
    value: str


class StateProvider(BaseModel, frozen=True):
    graph_uri: Annotated[str, SCHEME] = "file://./state/graph/"
    compiled_uri: Annotated[str, SCHEME] = "file://./state/compiled/"
    running_uri: Annotated[str, SCHEME] = "file://./state/run/"

    def list_graphs(self) -> list[str]:
        raise NotImplementedError("You need to use a provider class with a backend.")

    def list_compiled(self, graph_name: str) -> list[str]:
        raise NotImplementedError("You need to use a provider class with a backend.")

    def list_versions(self, graph_name: str, compiled_name: str) -> list[str]:
        raise NotImplementedError("You need to use a provider class with a backend.")

    def read_graph(self, graph_name: str) -> graph.Graph:
        raise NotImplementedError("You need to use a provider class with a backend.")

    def write_graph(self, graph_: graph.Graph):
        raise NotImplementedError("You need to use a provider class with a backend.")

    def read_compiled(self, graph_name: str, compiled_name: str) -> graph.CompiledGraph:
        raise NotImplementedError("You need to use a provider class with a backend.")

    def write_compiled(self, graph_name: str, compiled: graph.Graph):
        raise NotImplementedError("You need to use a provider class with a backend.")

    def read_running(
        self,
        graph_name: str,
        compiled_name: str,
        version: str,
    ) -> graph.RunningGraph:
        raise NotImplementedError("You need to use a provider class with a backend.")

    def write_running(
        self,
        graph_name: str,
        compiled_name: str,
        version: str,
        running: graph.RunningGraph,
    ):
        raise NotImplementedError("You need to use a provider class with a backend.")


class LocalStateProvider(StateProvider):
    def list_graphs(self) -> list[str]:
        return [
            f[: -len(".json")]
            for f in os.listdir(self.graph_uri[len("file://") :])
            if f.endswith(".json")
        ]

    def list_compiled(self, graph_name: str) -> list[str]:
        return [
            f[: -len(".json")]
            for f in os.listdir(
                posixpath.join(self.compiled_uri[len("file://") :], graph_name)
            )
            if f.endswith(".json")
        ]

    def list_versions(self, graph_name: str, compiled_name: str) -> list[str]:
        return [
            f[: -len(".json")]
            for f in os.listdir(
                posixpath.join(
                    self.running_uri[len("file://") :],
                    graph_name,
                    compiled_name,
                )
            )
            if f.endswith(".json")
        ]

    def read_graph(self, graph_name: str) -> graph.Graph:
        graph_file = posixpath.join(
            self.graph_uri[len("file://") :], f"{graph_name}.json"
        )
        try:
            with open(graph_file) as f:
                graph_json = json.loads(f.read())
        except (FileNotFoundError, json.JSONDecodeError):
            raise UnreadableGraph(value=graph_file)
        try:
            return graph.Graph.model_validate(graph_json)
        except ValidationError:
            raise IncompatibleGraph(value=json.dumps(graph_json, indent=2))

    def write_graph(self, graph_: graph.Graph):
        graph_file = posixpath.join(
            self.graph_uri[len("file://") :], f"{graph_.name}.json"
        )
        with open(graph_file, "w") as f:
            f.write(json.dumps(graph_.model_dump(), indent=2))

    def read_compiled(self, graph_name: str, compiled_name: str) -> graph.CompiledGraph:
        compiled_file = posixpath.join(
            self.compiled_uri[len("file://") :], graph_name, f"{compiled_name}.json"
        )
        try:
            with open(compiled_file) as f:
                compiled_json = json.loads(f.read())
        except (FileNotFoundError, json.JSONDecodeError):
            raise UnreadableGraph(value=compiled_file)
        try:
            return graph.CompiledGraph.model_validate(compiled_json)
        except ValidationError:
            raise IncompatibleGraph(value=json.dumps(compiled_json, indent=2))

    def write_compiled(self, graph_name: str, compiled: graph.Graph):
        compiled_file = posixpath.join(
            self.compiled_uri[len("file://") :], graph_name, f"{compiled.name}.json"
        )
        with open(compiled_file, "w") as f:
            f.write(json.dumps(compiled.model_dump(), indent=2))

    def read_running(
        self,
        graph_name: str,
        compiled_name: str,
        version: str,
    ) -> graph.RunningGraph:
        running_file = posixpath.join(
            self.running_uri[len("file://") :],
            graph_name,
            compiled_name,
            f"{version}.json",
        )
        try:
            with open(running_file) as f:
                running_json = json.loads(f.read())
        except (FileNotFoundError, json.JSONDecodeError):
            raise UnreadableGraph(value=running_file)
        try:
            return graph.Graph.model_validate(running_json)
        except ValidationError:
            raise IncompatibleGraph(value=json.dumps(running_json, indent=2))

    def write_running(
        self,
        graph_name: str,
        compiled_name: str,
        version: str,
        running: graph.Graph,
    ):
        running_file = posixpath.join(
            self.running_uri[len("file://") :],
            graph_name,
            compiled_name,
            f"{version}.json",
        )
        with open(running_file, "w") as f:
            f.write(json.dumps(running.model_dump(), indent=2))
