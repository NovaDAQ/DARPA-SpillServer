"""!
@file src/darpa_spillserver/cli.py
@brief Command-line entry point for the server (darpa-spill-server).

Command-line entry point for the server (``darpa-spill-server``).

Parses the command line, merges it with the YAML file and the environment (see
:mod:`darpa_spillserver.config`), and hands the resulting application to
uvicorn.

``--print-config`` renders the merged configuration and exits.  It is the
quickest way to answer "which file is this server actually reading, and what
did it end up with?", and it contacts nothing --- no TDU, no database --- so it
is safe to run against a production config.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional, Sequence

from . import __version__
from .config import Config, ConfigError, build_parser, configure_logging, load_config

__all__ = ["main", "run"]

log = logging.getLogger(__name__)


def _print_version() -> None:
    """!
    @brief Print the server version and the versions of its key runtime deps.

    @details Reports Python, nova-time-decoder (the NOvA time arithmetic used
    by novatime) and fastapi. A missing optional package is reported as
    "NOT INSTALLED" instead of raising, so --version works in a partially
    bootstrapped environment.
    """
    # Imports are local so that --version costs nothing on the normal path
    # and survives a broken install of any one dependency.
    import platform
    print("darpa-spill-server {}".format(__version__))
    print("Python {}".format(platform.python_version()))
    try:
        import nova_time_decoder
        print("nova-time-decoder {}".format(nova_time_decoder.__version__))
    except ImportError:
        print("nova-time-decoder NOT INSTALLED")
    try:
        import fastapi
        print("fastapi {}".format(fastapi.__version__))
    except ImportError:
        print("fastapi NOT INSTALLED")


def run(config: Config) -> int:
    """!
    @brief Start the HTTP server and block until it stops.

    @param config Fully merged and validated configuration.
    @return Process exit status: 0 on clean shutdown, 1 if uvicorn is not
            installed, 2 if a TLS file is unreadable or the application
            factory rejects the configuration or archive.
    """
    try:
        import uvicorn
    except ImportError:
        print(
            "error: uvicorn is not installed; run ./bootstrap.sh or "
            "'pip install -e .'",
            file=sys.stderr,
        )
        return 1

    from .app import create_app
    from .storage import StoreError

    # Check the TLS files here rather than in Config.validate(), so that
    # --print-config still works for someone who cannot read the private key.
    # uvicorn would otherwise fail on them with a bare traceback.
    # Only the TLS keys that pass the readability check are forwarded to
    # uvicorn; an empty dict means plain HTTP.
    ssl_options = {}
    if config.server.ssl_certfile:
        for key in ("ssl_certfile", "ssl_keyfile"):
            path = Path(getattr(config.server, key)).expanduser()
            try:
                # Open-and-close is the reliable readability test (os.access
                # ignores ACLs and effective-uid subtleties).
                path.open("rb").close()
            except OSError as exc:
                print("error: server.{}: cannot read {}: {}".format(
                    key, path, exc.strerror), file=sys.stderr)
                return 2
            ssl_options[key] = str(path)

    try:
        app = create_app(config)
    except (ConfigError, StoreError) as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    log.info(
        "serving on %s://%s:%d%s (archive: %s, sources: %s)",
        config.server.scheme, config.server.host, config.server.port,
        config.server.root_path or "", config.storage.path,
        ", ".join("{}={}".format(s.name, s.base_url)
                  for s in config.tdu.resolved_sources()),
    )
    if not config.auth.enabled:
        log.warning(
            "authentication is disabled; anyone who can reach this port can "
            "query the archive. Set auth.enabled to require Fermilab SSO."
        )

    # Access logging is tied to DEBUG: per-request lines are noise at the
    # normal level for a server polled continuously by clients.
    uvicorn.run(
        app,
        host=config.server.host,
        port=config.server.port,
        log_level=config.logging.level.lower(),
        access_log=config.logging.level.upper() == "DEBUG",
        **ssl_options,
    )
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """!
    @brief Entry point.  Returns a process exit status.

    @details Precedence of settings (command line > DARPA_SPILL_* environment
    > YAML file > defaults) is implemented by load_config(); this function
    only sequences --version, --print-config, logging setup and run().

    @param argv Argument vector without the program name; None means
                sys.argv[1:].
    @return Process exit status: 0 on success, interrupt, --version or
            --print-config; 2 on a configuration error; otherwise whatever
            run() returns.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # --version is answered before configuration is loaded so that it works
    # even when the config file is missing or invalid.
    if args.version:
        _print_version()
        return 0

    try:
        config = load_config(args=args)
    except ConfigError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    # Printing the configuration is pure inspection: it must not depend on
    # being able to open a log file, create a database, or reach a TDU, so it
    # runs before any of that is set up.
    if args.print_config:
        # default=str renders Path and other non-JSON values readably.
        print(json.dumps(config.to_dict(), indent=2, default=str))
        return 0

    configure_logging(config.logging)

    try:
        return run(config)
    except KeyboardInterrupt:
        log.info("interrupted; shutting down")
        return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
