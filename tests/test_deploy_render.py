from pathlib import Path
import shutil
import subprocess

import pytest

from deploy.render import render_systemd_unit


DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
SETTINGS = {
    "project_dir": "/opt/tron pipeline", "python": "/usr/bin/python3",
    "env_file": "/etc/tron/pipeline.env", "csv_dir": "/var/lib/tron/data", "user": "root",
}


@pytest.mark.parametrize("name", ["pipeline.service", "sync.service", "sync.timer"])
def test_render_shipped_templates(name):
    result = render_systemd_unit((DEPLOY / name).read_text(), SETTINGS)
    assert "${" not in result
    if name.endswith(".service"):
        assert 'WorkingDirectory=/opt/tron pipeline' in result
        assert 'ExecStart="/usr/bin/python3" -m pipeline.cli ' in result
        assert "User=root" in result
        assert "EnvironmentFile=/etc/tron/pipeline.env" in result


def test_render_escapes_systemd_specifiers():
    settings = dict(SETTINGS, csv_dir='/data/100%/files')
    assert render_systemd_unit('Environment="CSV_DIR=${csv_dir}"', settings) == 'Environment="CSV_DIR=/data/100%%/files"'


@pytest.mark.parametrize("key,value", [
    ("python", "python3"), ("project_dir", "C:/repo"), ("csv_dir", "/data/../etc"),
    ("env_file", "/etc/env\nExecStart=bad"), ("csv_dir", "/bad\x00path"),
    ("user", "root\nExecStart=bad"), ("user", "-root"),
    ("csv_dir", '/data/a"b'), ("csv_dir", '/data/a\\b'),
    ("python", "/bin/${PYTHON}"), ("env_file", "/etc/env "),
])
def test_render_rejects_invalid_deployment_values(key, value):
    with pytest.raises(ValueError, match=key):
        render_systemd_unit("${user}", dict(SETTINGS, **{key: value}))


@pytest.mark.parametrize("template", ["${unknown}", "${unfinished", "$?"])
def test_render_rejects_unknown_or_malformed_placeholders(template):
    with pytest.raises(ValueError, match="placeholder"):
        render_systemd_unit(template, SETTINGS)


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="requires Linux systemd-analyze")
def test_rendered_units_pass_systemd_verify(tmp_path):
    settings = dict(SETTINGS, python=shutil.which("python3"))
    paths = []
    for name in ("pipeline.service", "sync.service", "sync.timer"):
        path = tmp_path / name
        path.write_text(render_systemd_unit((DEPLOY / name).read_text(), settings))
        paths.append(str(path))
    result = subprocess.run(["systemd-analyze", "verify", *paths], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
