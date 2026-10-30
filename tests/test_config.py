"""!
@file tests/test_config.py
@brief Tests for configuration layering and validation.
"""

import pytest

from darpa_spillserver.config import (
    Config,
    ConfigError,
    build_parser,
    load_config,
)


def write_yaml(tmp_path, text):
    """!
    @brief Write a YAML configuration file for one test.
    @param tmp_path pytest per-test temporary directory.
    @param text YAML document body.
    @return path of the written spillserver.yaml as a string.
    """
    path = tmp_path / "spillserver.yaml"
    path.write_text(text)
    return str(path)


def test_defaults_are_usable_without_any_configuration():
    """!
    @brief With no file, environment or options the defaults validate: port
    8080, the ppc-01 TDU as the only source, authentication off.
    """
    config = load_config(argv=[], environ={}, search_paths=[])
    config.validate()
    assert config.server.port == 8080
    sources = config.tdu.resolved_sources()
    assert [s.base_url for s in sources] == ["http://tdu-near-master-ppc-01:8080"]
    assert sources[0].name == "tdu-near-master-ppc-01"
    assert config.auth.enabled is False, "Design.md: start with no authentication"


def test_yaml_overrides_defaults(tmp_path):
    """!
    @brief Values from a -c YAML file replace the defaults and Config.source
    records the file used.
    """
    path = write_yaml(tmp_path, """
server:
  port: 9999
tdu:
  base_url: http://other-tdu:8080
storage:
  path: /tmp/other.db
""")
    config = load_config(argv=["-c", path], environ={})
    assert config.server.port == 9999
    assert config.tdu.base_url == "http://other-tdu:8080"
    assert config.storage.path == "/tmp/other.db"
    assert config.source == path


def test_environment_overrides_yaml(tmp_path):
    """!
    @brief An environment variable outranks the YAML file.
    """
    path = write_yaml(tmp_path, "server:\n  port: 9999\n")
    config = load_config(
        argv=["-c", path], environ={"DARPA_SPILL_SERVER_PORT": "7777"}
    )
    assert config.server.port == 7777


def test_command_line_overrides_everything(tmp_path):
    """!
    @brief A command-line option outranks both the environment and the YAML
    file.
    """
    path = write_yaml(tmp_path, "server:\n  port: 9999\n")
    config = load_config(
        argv=["-c", path, "--port", "1234"],
        environ={"DARPA_SPILL_SERVER_PORT": "7777"},
    )
    assert config.server.port == 1234


def test_options_left_off_the_command_line_do_not_clobber_yaml(tmp_path):
    """!
    @brief Argparse defaults must be None, or every flag would override the
    file.
    """
    path = write_yaml(tmp_path, "server:\n  host: 127.0.0.1\n  port: 9999\n")
    config = load_config(argv=["-c", path, "--port", "1234"], environ={})
    assert config.server.host == "127.0.0.1", "host was not on the command line"
    assert config.server.port == 1234


def test_missing_explicit_config_file_is_an_error():
    """!
    @brief A -c path that does not exist raises ConfigError ('not found')
    rather than falling back.
    """
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["-c", "/nonexistent/spillserver.yaml"], environ={})
    assert "not found" in str(excinfo.value)


def test_unknown_section_is_rejected(tmp_path):
    """!
    @brief An unknown top-level YAML section raises ConfigError.
    """
    path = write_yaml(tmp_path, "nonsense:\n  key: value\n")
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["-c", path], environ={})
    assert "unknown configuration section" in str(excinfo.value)


def test_unknown_key_is_rejected_not_ignored(tmp_path):
    """!
    @brief A silently ignored 'retention_day' typo is noticed only after a
    prune.
    """
    path = write_yaml(tmp_path, "storage:\n  retention_day: 30\n")
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["-c", path], environ={})
    message = str(excinfo.value)
    assert "retention_day" in message
    assert "retention_days" in message, "the error should suggest the real key"


def test_malformed_yaml_is_reported_clearly(tmp_path):
    """!
    @brief A YAML syntax error is reported as ConfigError 'not valid YAML'.
    """
    path = write_yaml(tmp_path, "server:\n  port: [unclosed\n")
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["-c", path], environ={})
    assert "not valid YAML" in str(excinfo.value)


def test_empty_yaml_file_is_fine(tmp_path):
    """!
    @brief An empty file is treated as no overrides.
    """
    path = write_yaml(tmp_path, "")
    config = load_config(argv=["-c", path], environ={})
    assert config.server.port == 8080


def test_wrong_type_is_reported_with_the_setting_name(tmp_path):
    """!
    @brief A value that cannot be coerced names the dotted setting
    (server.port).
    """
    path = write_yaml(tmp_path, "server:\n  port: not-a-number\n")
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["-c", path], environ={})
    assert "server.port" in str(excinfo.value)


def test_booleans_accept_yaml_and_string_spellings(tmp_path):
    """!
    @brief Booleans accept YAML false and the environment string 'no'.
    """
    path = write_yaml(tmp_path, "ingest:\n  enabled: false\n")
    assert load_config(argv=["-c", path], environ={}).ingest.enabled is False

    config = load_config(argv=[], environ={"DARPA_SPILL_INGEST_ENABLED": "no"},
                         search_paths=[])
    assert config.ingest.enabled is False


def test_lists_accept_yaml_lists_and_comma_strings(tmp_path):
    """!
    @brief List settings accept a YAML list or a comma-separated environment
    string.
    """
    path = write_yaml(tmp_path, "server:\n  cors_origins:\n    - https://a\n    - https://b\n")
    assert load_config(argv=["-c", path], environ={}).server.cors_origins == [
        "https://a", "https://b"
    ]

    config = load_config(
        argv=[], search_paths=[],
        environ={"DARPA_SPILL_SERVER_CORS_ORIGINS": "https://a,https://b"},
    )
    assert config.server.cors_origins == ["https://a", "https://b"]


def test_no_ingest_flag(tmp_path):
    """!
    @brief --no-ingest disables ingest.
    """
    config = load_config(argv=["--no-ingest"], environ={}, search_paths=[])
    assert config.ingest.enabled is False


def test_verbose_flag_sets_debug():
    """!
    @brief -v sets the log level to DEBUG.
    """
    config = load_config(argv=["-v"], environ={}, search_paths=[])
    assert config.logging.level == "DEBUG"


# ------------------------------------------------------------- validation


@pytest.mark.parametrize("argv,fragment", [
    (["--port", "0"], "server.port"),
    (["--tdu-url", "ftp://tdu"], "tdu.base_url"),
    (["--interval", "0"], "ingest.interval"),
    (["--batch-limit", "0"], "ingest.batch_limit"),
    (["--overlap", "-1"], "ingest.overlap"),
    (["--retention-days", "-5"], "retention_days"),
    (["--timezone", "Mars/Olympus_Mons"], "timezone"),
])
def test_invalid_values_are_rejected(argv, fragment):
    """!
    @brief Out-of-range or malformed values raise ConfigError naming the
    offending setting.
    @param argv command line carrying one invalid value.
    @param fragment setting name expected in the error message.
    """
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=argv, environ={}, search_paths=[])
    assert fragment in str(excinfo.value)


@pytest.mark.parametrize("argv", [
    ["--ssl-certfile", "/etc/pki/spill.crt"],
    ["--ssl-keyfile", "/etc/pki/spill.key"],
])
def test_tls_needs_both_certificate_and_key(argv):
    """!
    @brief Supplying only one of the TLS certificate and key raises
    ConfigError.
    @param argv command line with only one TLS option.
    """
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=argv, environ={}, search_paths=[])
    assert "ssl_certfile" in str(excinfo.value)


def test_tls_settings_select_https():
    """!
    @brief Certificate (CLI) plus key (env) together select the https scheme;
    the default scheme is http.
    """
    config = load_config(
        argv=["--ssl-certfile", "/etc/pki/spill.crt"],
        environ={"DARPA_SPILL_SERVER_SSL_KEYFILE": "/etc/pki/spill.key"},
        search_paths=[],
    )
    assert config.server.ssl_keyfile == "/etc/pki/spill.key"
    assert config.server.scheme == "https"
    assert Config().server.scheme == "http"


def test_status_dir_is_off_by_default():
    """!
    @brief server.status_dir defaults to the empty string (no /status mount).
    """
    assert load_config(argv=[], environ={}, search_paths=[]).server.status_dir == ""


def test_status_dir_layers(tmp_path):
    """!
    @brief status_dir follows the precedence CLI > environment (alias
    DARPA_SPILL_STATUS_DIR) > YAML.
    """
    path = write_yaml(tmp_path, "server:\n  status_dir: /from/yaml\n")
    assert load_config(argv=["-c", path], environ={}).server.status_dir == (
        "/from/yaml"
    )
    config = load_config(
        argv=["-c", path], environ={"DARPA_SPILL_STATUS_DIR": "/from/env"}
    )
    assert config.server.status_dir == "/from/env"
    config = load_config(
        argv=["-c", path, "--status-dir", "/from/cli"],
        environ={"DARPA_SPILL_STATUS_DIR": "/from/env"},
    )
    assert config.server.status_dir == "/from/cli"


def test_status_dir_canonical_env_name_beats_the_alias():
    """!
    @brief When both are set, DARPA_SPILL_SERVER_STATUS_DIR outranks the
    DARPA_SPILL_STATUS_DIR alias.
    """
    config = load_config(
        argv=[],
        environ={
            "DARPA_SPILL_STATUS_DIR": "/alias",
            "DARPA_SPILL_SERVER_STATUS_DIR": "/canonical",
        },
        search_paths=[],
    )
    assert config.server.status_dir == "/canonical"


def test_max_limit_below_default_limit_is_rejected():
    """!
    @brief query.max_limit smaller than query.default_limit raises ConfigError.
    """
    with pytest.raises(ConfigError):
        load_config(argv=["--default-limit", "100", "--max-limit", "50"],
                    environ={}, search_paths=[])


def test_enabling_auth_requires_its_settings():
    """!
    @brief --auth without issuer and client_id raises ConfigError naming both.
    """
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["--auth"], environ={}, search_paths=[])
    message = str(excinfo.value)
    assert "issuer" in message and "client_id" in message


def test_enabling_auth_requires_a_session_secret(tmp_path):
    """!
    @brief Auth configured without session_secret raises ConfigError naming it.
    """
    path = write_yaml(tmp_path, """
auth:
  enabled: true
  issuer: https://pingprod.fnal.gov/idp
  client_id: spillserver
  client_secret: hunter2
  redirect_url: https://example.fnal.gov/auth/callback
""")
    with pytest.raises(ConfigError) as excinfo:
        load_config(argv=["-c", path], environ={})
    assert "session_secret" in str(excinfo.value)


def test_fully_configured_auth_validates(tmp_path):
    """!
    @brief A complete auth section validates and enables auth.
    """
    path = write_yaml(tmp_path, """
auth:
  enabled: true
  issuer: https://pingprod.fnal.gov/idp
  client_id: spillserver
  client_secret: hunter2
  redirect_url: https://example.fnal.gov/auth/callback
  session_secret: 0123456789abcdef
""")
    config = load_config(argv=["-c", path], environ={})
    assert config.auth.enabled is True


def test_secrets_are_redacted_in_the_dict_view(tmp_path):
    """!
    @brief to_dict() redacts client_secret and session_secret; redact=False
    exposes them.
    """
    path = write_yaml(tmp_path, """
auth:
  enabled: true
  issuer: https://pingprod.fnal.gov/idp
  client_id: spillserver
  client_secret: hunter2
  redirect_url: https://example.fnal.gov/auth/callback
  session_secret: 0123456789abcdef
""")
    config = load_config(argv=["-c", path], environ={})
    data = config.to_dict()
    assert data["auth"]["client_secret"] == "***redacted***"
    assert data["auth"]["session_secret"] == "***redacted***"
    assert config.to_dict(redact=False)["auth"]["client_secret"] == "hunter2"


def test_client_secret_can_come_from_a_file(tmp_path):
    """!
    @brief client_secret_file is read and stripped of its trailing newline.
    """
    secret = tmp_path / "secret.txt"
    secret.write_text("from-a-file\n")
    config = Config()
    config.auth.client_secret_file = str(secret)
    assert config.auth.resolved_client_secret() == "from-a-file"


def test_parser_help_mentions_every_group():
    """!
    @brief --help lists every option group.
    """
    help_text = build_parser().format_help()
    for heading in ("server", "TDU data source", "ingest", "storage",
                    "query defaults", "authentication", "logging"):
        assert heading in help_text
