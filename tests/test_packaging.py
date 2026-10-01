"""Repository conventions that nothing else would catch drifting.

Each check here encodes a rule the project follows --- requirements files
mirror pyproject.toml, every executable has a man page stamped with the
current version, every platform has its scripts --- so that breaking one
fails the suite instead of being noticed at the next release.
"""

import os
import re
from pathlib import Path

import pytest

from darpa_spillserver import __version__

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = (ROOT / "pyproject.toml").read_text()


def _section(name):
    match = re.search(r"^\[{}\]\n(.*?)(?=^\[|\Z)".format(re.escape(name)),
                      PYPROJECT, re.M | re.S)
    assert match, name
    return match.group(1)


def _requirements(path):
    lines = (ROOT / path).read_text().splitlines()
    return sorted(l.strip() for l in lines if l.strip() and not l.startswith("#"))


def _quoted(text):
    return re.findall(r'"([^"]+)"', text)


def test_versions_agree():
    assert re.search(r'^version = "{}"$'.format(re.escape(__version__)), PYPROJECT, re.M)
    cmake = ROOT / "CMakeLists.txt"
    if cmake.exists():
        assert re.search(r"VERSION\s+{}\b".format(re.escape(__version__)),
                         cmake.read_text())


def test_requirements_mirror_pyproject():
    project = _section("project")
    deps = _quoted(project.split("dependencies = [", 1)[1].split("]\n", 1)[0])
    assert _requirements("requirements.txt") == sorted(deps)

    extras = _section("project.optional-dependencies")
    wanted = set()
    for line in extras.splitlines():
        if line.startswith(("test", "oidc")):
            wanted.update(_quoted(line))
    assert _requirements("requirements-dev.txt") == sorted(wanted)


def test_every_console_script_has_a_man_page():
    scripts = re.findall(r"^([a-z0-9-]+) = ", _section("project.scripts"), re.M)
    assert "darpa-spill-client" in scripts
    for name in scripts:
        assert (ROOT / "man" / "{}.1".format(name)).is_file(), name


@pytest.mark.parametrize("script", ["bootstrap", "start-darpa-spillserver",
                                    "stop-darpa-spillserver"])
def test_scripts_exist_for_every_platform(script):
    shell = ROOT / "{}.sh".format(script)
    assert shell.is_file()
    assert (ROOT / "{}.ps1".format(script)).is_file()
    if os.name == "posix":
        assert os.access(str(shell), os.X_OK), shell
    page = "darpa-spillserver-bootstrap" if script == "bootstrap" else script
    assert (ROOT / "man" / "{}.1".format(page)).is_file()


def test_man_pages_carry_the_current_version():
    pages = sorted((ROOT / "man").glob("*.[13]"))
    assert pages
    for page in pages:
        header = next(l for l in page.read_text().splitlines() if l.startswith(".TH"))
        assert __version__ in header, page.name


def test_shell_scripts_do_not_rely_on_linux_only_tools():
    for script in ROOT.glob("*.sh"):
        text = script.read_text()
        assert "/proc/" not in text, script.name       # absent on macOS
        assert not re.search(r"^\s*setsid\b", text, re.M), script.name


def test_local_secrets_are_ignored():
    ignored = (ROOT / ".gitignore").read_text().splitlines()
    assert ".env" in ignored
