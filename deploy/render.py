"""Render Linux service templates from explicit host deployment settings."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
import re
from string import Template


def render_systemd_unit(template: str, settings: Mapping[str, str]) -> str:
    """Substitute deployment paths and user into a systemd unit template.

    Deployment settings are separate from environment-derived pipeline Settings:
    ``project_dir``, ``python``, ``env_file``, ``csv_dir`` are absolute Linux
    paths and ``user`` is the service account. Percent signs are escaped as
    literal systemd specifiers; quotes/backslashes/dollars are rejected so values work
    in both raw path directives and quoted command/environment directives.
    Paths need not exist on the machine doing the rendering.
    """
    values = {}
    for key in ("project_dir", "python", "env_file", "csv_dir"):
        value = settings.get(key, "")
        if (
            not isinstance(value, str)
            or not value.startswith("/")
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            or value != value.strip()
            or any(char in value for char in ('"', "\\", "$"))
            or ".." in PurePosixPath(value).parts
        ):
            raise ValueError(f"{key} must be an absolute Linux path without surrounding whitespace, control characters, quotes, backslashes, dollars or '..'")
        values[key] = value.replace("%", "%%")
    user = settings.get("user", "")
    if not isinstance(user, str) or not re.fullmatch(r"[a-z_][a-z0-9_-]*\$?", user):
        raise ValueError("user must be a Linux service account name")
    values["user"] = user
    try:
        return Template(template).substitute(values)
    except (KeyError, ValueError) as error:
        raise ValueError("Unit template contains an unknown or invalid placeholder") from error
