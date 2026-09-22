"""Authentication hooks, wired for Fermilab OIDC single sign-on.

Design.md asks for the server to start with no authentication but to carry the
right hooks for enabling OIDC later.  That is what this module is: a single
:class:`Authenticator` interface with two implementations, chosen by
configuration rather than by editing code.

:class:`NoAuth`
    The default.  Every request is allowed and reports an anonymous
    :class:`Principal`.

:class:`OIDCAuthenticator`
    Authorization-code flow against an OIDC provider --- at Fermilab, the
    PingFederate service whose issuer is typically
    ``https://pingprod.fnal.gov/idp``.  Enabled with ``auth.enabled: true``.

Because the interface is the same either way, every route already declares its
dependency on :func:`Authenticator.require_user`.  Turning SSO on is a config
change and a restart; no route is rewritten, and no route can be forgotten.

The ``authlib`` and ``itsdangerous`` dependencies are needed only for the OIDC
path and live in the ``oidc`` extra, so a deployment that never enables SSO
does not install them.  Their absence is reported at startup, when it can be
fixed, rather than at the first login attempt.
"""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from .config import AuthConfig

__all__ = [
    "Principal",
    "Authenticator",
    "NoAuth",
    "OIDCAuthenticator",
    "AuthError",
    "build_authenticator",
    "ANONYMOUS",
]

log = logging.getLogger(__name__)


class AuthError(RuntimeError):
    """Raised when authentication is misconfigured or cannot be completed."""


@dataclass(frozen=True)
class Principal:
    """Who is making a request."""

    subject: str
    """Stable identifier from the provider (the ``sub`` claim), or
    ``"anonymous"``."""

    username: str = ""
    email: str = ""
    display_name: str = ""
    groups: List[str] = field(default_factory=list)
    authenticated: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "subject": self.subject,
            "username": self.username,
            "email": self.email,
            "display_name": self.display_name,
            "groups": list(self.groups),
            "authenticated": self.authenticated,
        }


#: The principal used when authentication is disabled.
ANONYMOUS = Principal(subject="anonymous", display_name="anonymous")


class Authenticator:
    """Interface every authentication backend implements."""

    #: Whether this backend actually challenges callers.
    enabled: bool = False

    #: Human-readable name for the status endpoint.
    name: str = "none"

    async def startup(self) -> None:
        """Perform any one-time setup, such as OIDC discovery."""

    async def shutdown(self) -> None:
        """Release any resources held by the backend."""

    async def authenticate(self, request) -> Principal:
        """Identify the caller behind *request*.

        Implementations return :data:`ANONYMOUS` rather than raising when no
        credential is present; :meth:`require_user` decides whether that is
        acceptable.
        """
        raise NotImplementedError

    async def require_user(self, request) -> Principal:
        """Return the caller's principal, or raise 401/403.

        This is the FastAPI dependency every protected route uses.
        """
        raise NotImplementedError

    def register_routes(self, app, prefix: str = "/auth") -> None:
        """Add any login/callback/logout routes this backend needs."""

    def describe(self) -> Dict[str, Any]:
        """Configuration summary for ``/api/status``, with no secrets."""
        return {"enabled": self.enabled, "provider": self.name}


class NoAuth(Authenticator):
    """Allows every request; the default, as Design.md specifies."""

    enabled = False
    name = "none"

    async def authenticate(self, request) -> Principal:
        return ANONYMOUS

    async def require_user(self, request) -> Principal:
        return ANONYMOUS


class OIDCAuthenticator(Authenticator):
    """OIDC authorization-code flow with a signed session cookie.

    Two credential styles are accepted, because both are needed in practice:
    a browser session cookie established by the login flow, and an
    ``Authorization: Bearer <id_token>`` header for scripted API clients that
    obtained a token elsewhere.
    """

    enabled = True
    name = "oidc"

    def __init__(self, config: AuthConfig) -> None:
        self.config = config
        self._oauth = None
        self._client = None
        self._serializer = None
        self._metadata: Dict[str, Any] = {}

    # -- lifecycle ----------------------------------------------------------

    async def startup(self) -> None:
        try:
            from authlib.integrations.starlette_client import OAuth
        except ImportError:
            raise AuthError(
                "auth.enabled is true but the OIDC dependencies are missing; "
                "install them with: pip install -e '.[oidc]'"
            ) from None

        try:
            from itsdangerous import URLSafeTimedSerializer
        except ImportError:
            raise AuthError(
                "auth.enabled is true but itsdangerous is missing; "
                "install it with: pip install -e '.[oidc]'"
            ) from None

        secret = self.config.session_secret
        if not secret:
            raise AuthError(
                "auth.session_secret is unset; generate one with: "
                "openssl rand -hex 32"
            )
        self._serializer = URLSafeTimedSerializer(secret, salt="darpa-spill-session")

        self._oauth = OAuth()
        self._oauth.register(
            name="fnal",
            client_id=self.config.client_id,
            client_secret=self.config.resolved_client_secret(),
            server_metadata_url=(
                self.config.issuer.rstrip("/")
                + "/.well-known/openid-configuration"
            ),
            client_kwargs={"scope": " ".join(self.config.scopes)},
        )
        self._client = self._oauth.create_client("fnal")
        log.info("OIDC enabled; issuer %s", self.config.issuer)

    # -- credentials --------------------------------------------------------

    def _principal_from_claims(self, claims: Dict[str, Any]) -> Principal:
        groups = claims.get("groups") or claims.get("memberOf") or []
        if isinstance(groups, str):
            groups = [groups]
        return Principal(
            subject=str(claims.get("sub", "")),
            username=str(claims.get("preferred_username", claims.get("upn", ""))),
            email=str(claims.get("email", "")),
            display_name=str(claims.get("name", "")),
            groups=[str(g) for g in groups],
            authenticated=True,
        )

    async def authenticate(self, request) -> Principal:
        header = request.headers.get("authorization", "")
        if header.lower().startswith("bearer "):
            token = header[7:].strip()
            claims = await self._verify_id_token(token)
            if claims is not None:
                return self._principal_from_claims(claims)
            return ANONYMOUS

        cookie = request.cookies.get(self.config.session_cookie)
        if cookie and self._serializer is not None:
            from itsdangerous import BadSignature, SignatureExpired
            try:
                claims = self._serializer.loads(
                    cookie, max_age=self.config.session_max_age
                )
            except SignatureExpired:
                log.debug("session cookie expired")
                return ANONYMOUS
            except BadSignature:
                log.warning("rejected a session cookie with a bad signature")
                return ANONYMOUS
            return self._principal_from_claims(claims)

        return ANONYMOUS

    async def _verify_id_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate a bearer ID token against the provider's JWKS."""
        if self._client is None:
            return None
        try:
            metadata = await self._client.load_server_metadata()
            jwks = await self._client.fetch_jwk_set()
            from authlib.jose import JsonWebToken

            jwt = JsonWebToken(["RS256", "ES256"])
            claims = jwt.decode(
                token,
                jwks,
                claims_options={
                    "iss": {"essential": True, "value": metadata["issuer"]},
                    "aud": {"essential": True, "value": self.config.client_id},
                },
            )
            claims.validate()
            return dict(claims)
        except Exception as exc:
            log.debug("bearer token rejected: %s", exc)
            return None

    async def require_user(self, request) -> Principal:
        from fastapi import HTTPException, status

        principal = await self.authenticate(request)
        if not principal.authenticated:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required; sign in at /auth/login",
                headers={"WWW-Authenticate": "Bearer"},
            )

        allowed = self.config.allowed_groups
        if allowed and not set(allowed) & set(principal.groups):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "your account is not in a group permitted to use this "
                    "server (need one of: {})".format(", ".join(allowed))
                ),
            )
        return principal

    # -- routes -------------------------------------------------------------

    def register_routes(self, app, prefix: str = "/auth") -> None:
        from fastapi import HTTPException, Request
        from fastapi.responses import JSONResponse, RedirectResponse

        @app.get(prefix + "/login", include_in_schema=False)
        async def login(request: Request, next: str = "/"):
            if self._client is None:
                raise HTTPException(503, "OIDC is not initialised")
            # The nonce guards against token replay; it is carried in the
            # Starlette session and checked on the way back.
            nonce = secrets.token_urlsafe(16)
            request.session["oidc_nonce"] = nonce
            request.session["post_login_redirect"] = next
            redirect_uri = self.config.redirect_url or str(
                request.url_for("auth_callback")
            )
            return await self._client.authorize_redirect(
                request, redirect_uri, nonce=nonce
            )

        @app.get(prefix + "/callback", name="auth_callback", include_in_schema=False)
        async def callback(request: Request):
            if self._client is None:
                raise HTTPException(503, "OIDC is not initialised")
            try:
                token = await self._client.authorize_access_token(request)
            except Exception as exc:
                log.warning("OIDC callback failed: %s", exc)
                raise HTTPException(401, "single sign-on failed: {}".format(exc))

            nonce = request.session.pop("oidc_nonce", None)
            claims = token.get("userinfo")
            if claims is None:
                claims = await self._client.parse_id_token(token, nonce=nonce)

            principal = self._principal_from_claims(dict(claims))
            target = request.session.pop("post_login_redirect", "/")
            response = RedirectResponse(url=target, status_code=303)
            response.set_cookie(
                self.config.session_cookie,
                self._serializer.dumps(dict(claims)),
                max_age=self.config.session_max_age,
                httponly=True,
                samesite="lax",
                secure=str(request.url.scheme) == "https",
            )
            log.info("signed in: %s", principal.username or principal.subject)
            return response

        @app.get(prefix + "/logout", include_in_schema=False)
        async def logout(request: Request):
            response = RedirectResponse(url="/", status_code=303)
            response.delete_cookie(self.config.session_cookie)
            request.session.clear()
            return response

        @app.get(prefix + "/whoami", include_in_schema=False)
        async def whoami(request: Request):
            principal = await self.authenticate(request)
            return JSONResponse(principal.as_dict())

    def describe(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "provider": self.name,
            "issuer": self.config.issuer,
            "client_id": self.config.client_id,
            "scopes": list(self.config.scopes),
            "allowed_groups": list(self.config.allowed_groups),
        }


def build_authenticator(config: AuthConfig) -> Authenticator:
    """Return the backend selected by *config*."""
    if not config.enabled:
        return NoAuth()
    if config.provider.lower() in ("oidc", "openid", "fermilab", "fnal"):
        return OIDCAuthenticator(config)
    raise AuthError(
        "unknown auth.provider {!r}; supported: oidc".format(config.provider)
    )
