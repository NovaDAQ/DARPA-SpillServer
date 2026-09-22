"""Command-line entry point for the server (``darpa-spill-server``).

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
from typing import Optional, Sequence

from . import __version__
from .config import Config, ConfigError, build_parser, configure_logging, load_config

__all__ = ["main", "run"]

log = logging.getLogger(__name__)


def _print_version() -> None:
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
    """Start the HTTP server and block until it stops."""
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

    app = create_app(config)

    log.info(
        "serving on http://%s:%d%s (archive: %s, TDU: %s)",
        config.server.host, config.server.port, config.server.root_path or "",
        config.storage.path, config.tdu.base_url,
    )
    if not config.auth.enabled:
        log.warning(
            "authentication is disabled; anyone who can reach this port can "
            "query the archive. Set auth.enabled to require Fermilab SSO."
        )

    uvicorn.run(
        app,
        host=config.server.host,
        port=config.server.port,
        log_level=config.logging.level.lower(),
        access_log=config.logging.level.upper() == "DEBUG",
    )
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point.  Returns a process exit status."""
    parser = build_parser()
    args = parser.parse_args(argv)

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
