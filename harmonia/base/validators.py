from pydantic.functional_validators import AfterValidator


def has_name(uri: str) -> str:
    assert "{name}" in uri, "URI must contain a name indicator ({name})"
    return uri


def has_version(uri: str) -> str:
    assert "{version}" in uri, "URI must contain a version indicator ({version})"
    return uri


def has_scheme(uri: str) -> str:
    assert "://" in uri, "URI must contain a protocol"
    return uri


def is_file_scheme(uri: str):
    assert uri.startswith("file://"), "Local URI must start with 'file://'"
    return uri


NAME = AfterValidator(has_name)
VERSION = AfterValidator(has_version)
SCHEME = AfterValidator(has_scheme)
FILE_SCHEME = AfterValidator(is_file_scheme)