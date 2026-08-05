"""Docker-style image name and tag handling for Glance images."""

from __future__ import annotations

import re
from dataclasses import dataclass


class ImageReferenceError(ValueError):
    """Raised when an image reference cannot be normalized safely."""


_TAG_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(?::[0-9]{1,5})?(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*$")


@dataclass(frozen=True, slots=True)
class ImageReference:
    repository: str
    tag: str

    @property
    def name(self) -> str:
        return f"{self.repository}:{self.tag}"


def parse_image_reference(name: str) -> ImageReference:
    """Parse a Docker-style repository reference, defaulting a missing tag."""
    raw = name.strip()
    if not raw:
        raise ImageReferenceError("이미지 이름이 비어 있습니다")
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in raw):
        raise ImageReferenceError("이미지 이름에는 공백이나 제어 문자를 사용할 수 없습니다")
    if "@" in raw:
        raise ImageReferenceError("digest 기반 이미지 이름은 지원하지 않습니다")

    slash_index = raw.rfind("/")
    colon_index = raw.rfind(":")
    if colon_index > slash_index:
        repository = raw[:colon_index]
        tag = raw[colon_index + 1 :]
    else:
        repository = raw
        tag = "latest"

    if not _REPOSITORY_RE.fullmatch(repository):
        raise ImageReferenceError("이미지 저장소 이름이 올바르지 않습니다")
    if not repository or repository.endswith("/") or "//" in repository:
        raise ImageReferenceError("이미지 저장소 이름이 올바르지 않습니다")
    if not _TAG_RE.fullmatch(tag):
        raise ImageReferenceError("이미지 tag 형식이 올바르지 않습니다")
    return ImageReference(repository=repository, tag=tag)


def normalize_image_reference(name: str) -> str:
    """Return the canonical `repository:tag` value for a new or renamed image."""
    return parse_image_reference(name).name


def image_reference_fields(name: str | None) -> tuple[str, str, str]:
    """Return canonical display name, repository, and tag for Glance data.

    Existing deployments can contain legacy names that predate validation. Keep
    those readable and make their implicit `latest` tag visible until an
    operator renames them.
    """
    raw = (name or "").strip()
    if not raw:
        return "", "", "latest"
    try:
        reference = parse_image_reference(raw)
    except ImageReferenceError:
        return f"{raw}:latest", raw, "latest"
    return reference.name, reference.repository, reference.tag
