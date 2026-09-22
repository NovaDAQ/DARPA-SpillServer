"""darpa_spillserver -- serve NOvA accelerator event timestamps from a TDU.

The DARPA Spill Information Server polls the embedded bottle server running on
a NOvA TDU, archives the decoded accelerator events it reports, and serves them
to clients as a structured table selectable by time range and by accelerator
signal.  Results are available as CSV or JSON, with every timestamp rendered in
NOvA base time, UNIX, UTC and GPS.

Typical use::

    from darpa_spillserver.config import load_config
    from darpa_spillserver.app import create_app

    app = create_app(load_config(argv=["--database", "spills.db"]))

or, from the command line::

    darpa-spill-server -c config/spillserver.yaml

The modules are layered so each can be used on its own:

:mod:`~darpa_spillserver.signals`
    The accelerator signal registry and the hardware's decode rules.
:mod:`~darpa_spillserver.novatime`
    Parsing human time expressions and converting between timescales.
:mod:`~darpa_spillserver.storage`
    The SQLite event archive.
:mod:`~darpa_spillserver.tdu_client`
    The HTTP client for the TDU's bottle server.
:mod:`~darpa_spillserver.poller`
    The background ingest loop.
:mod:`~darpa_spillserver.formats`
    CSV and JSON rendering.
:mod:`~darpa_spillserver.api`, :mod:`~darpa_spillserver.app`
    The HTTP surface.
"""

__version__ = "1.0.0"

__all__ = ["__version__"]
