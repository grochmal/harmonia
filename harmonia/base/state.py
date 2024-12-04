import json
import os
import posixpath
from typing import Annotated

from pydantic import BaseModel

from harmonia.base import graph
from harmonia.base.validators import COMPILED, GRAPH, SCHEME, VERSION


class StateProvider(BaseModel, frozen=True):
    graph_uri: Annotated[str, GRAPH, SCHEME] = "file://./state/graph/"
    compiled_uri: Annotated[str, GRAPH, COMPILED, SCHEME] = "file://./state/compiled/"
    running_uri: Annotated[str, GRAPH, COMPILED, VERSION, SCHEME] = (
        "file://./state/run/"
    )

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
        with open(graph_file) as f:
            return graph.Graph.model_validate(json.loads(f.read()))
