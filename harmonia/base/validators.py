import os
from typing import TypeVar

from pydantic.functional_validators import AfterValidator

T = TypeVar("T")


def ensure_unique_elements(elements: tuple[T, ...]) -> tuple[T, ...]:
    unique_elements = set(elements)
    assert len(unique_elements) == len(elements), "Elements must be unique"
    return tuple(sorted(unique_elements))


def makedirs(uri: str):
    if not uri.startswith("file://"):
        # remote stores do not require parent dir handling
        return
    dirs, _ = os.path.split(uri[len("file://") :])
    if dirs:
        os.makedirs(dirs, exist_ok=True)


def has_name(uri: str) -> str:
    assert "{name}" in uri, "URI must contain a name indicator ({name})"
    return uri


def has_version(uri: str) -> str:
    assert "{version}" in uri, "URI must contain a version indicator ({version})"
    return uri


def has_scheme(uri: str) -> str:
    assert "://" in uri, "URI must contain a protocol"
    return uri


def is_file_scheme(uri: str) -> str:
    assert uri.startswith("file://"), "Local URI must start with 'file://'"
    return uri


NAME = AfterValidator(has_name)
VERSION = AfterValidator(has_version)
SCHEME = AfterValidator(has_scheme)
FILE_SCHEME = AfterValidator(is_file_scheme)
UNIQUE_ELEMENTS = AfterValidator(ensure_unique_elements)
