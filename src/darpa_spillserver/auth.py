"""!
@file src/darpa_spillserver/auth.py
@brief Pluggable authentication: NoAuth (default) and OIDC single sign-on with session cookie or bearer ID token.

Authentication hooks, wired for Fermilab OIDC single sign-on.

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

Token handling at a glance (OIDC):

- Browser: ``/auth/login`` -> provider -> ``/auth/callback``, which stores the
  ID-token claims in a cookie (``auth.session_cookie``) signed with
  ``auth.session_secret`` via itsdangerous and valid for
  ``auth.session_max_age`` seconds.  The cookie is signed, not encrypted.
- Scripts: ``Authorization: Bearer <id_token>``, verified per request
  against the provider's JWKS (RS256/ES256), with ``iss`` and ``aud``
  (= ``auth.client_id``) required.
- Either way, ``auth.allowed_groups`` (if non-empty) is enforced in
  require_user against the ``groups``/``memberOf`` claim.

The admin token for changing sources is a separate mechanism, handled in
:mod:`darpa_spillserver.api` (``X-Admin-Token``), not here.
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

## Module logger.
log = logging.getLogger(__name__)


class AuthError(RuntimeError):
    """!
    @brief Raised when authentication is misconfigured or cannot be completed.
    """


@dataclass(frozen=True)
class Principal:
    """!
    @brief Who is making a request.

    Immutable; built from OIDC claims or the ANONYMOUS constant.
    """

    ## Stable identifier from the provider (the ``sub`` claim), or
    # ``"anonymous"``.
    subject: str

    ## ``preferred_username`` (or ``upn``) claim.
    username: str = ""
    ## ``email`` claim.
    email: str = ""
    ## ``name`` claim.
    display_name: str = ""
    ## ``groups`` (or ``memberOf``) claim, as strings.
    groups: List[str] = field(default_factory=list)
    ## True only for a verified bearer token or a valid session cookie.
    authenticated: bool = False

    def as_dict(self) -> Dict[str, Any]:
        """!
        @brief JSON-ready rendering, used by ``/auth/whoami``.
        @return the fields as a dict.
        """
        return {
            "subject": self.subject,
            "username": self.username,
            "email": self.email,
            "display_name": self.display_name,
            "groups": list(self.groups),
            "authenticated": self.authenticated,
        }


## The principal used when authentication is disabled.
ANONYMOUS = Principal(subject="anonymous", display_name="anonymous")


class Authenticator:
    """!
    @brief Interface every authentication backend implements.
    """

    ## Whether this backend actually challenges callers.
    enabled: bool = False

    ## Human-readable name for the status endpoint.
    name: str = "none"

    async def startup(self) -> None:
        """!
        @brief Perform any one-time setup, such as OIDC discovery.

        Called from the application lifespan before serving.
        """

    async def shutdown(self) -> None:
        """!
        @brief Release any resources held by the backend.
        """

    async def authenticate(self, request) -> Principal:
        """!
        @brief Identify the caller behind *request*.

        Implementations return :data:`ANONYMOUS` rather than raising when no
        credential is present; :meth:`require_user` decides whether that is
        acceptable.

        @param request the Starlette request.
        @return the caller's principal.
        @throws NotImplementedError in the base class.
        """
        raise NotImplementedError

    async def require_user(self, request) -> Principal:
        """!
        @brief Return the caller's principal, or raise 401/403.

        This is the FastAPI dependency every protected route uses.

        @param request the Starlette request.
        @return the caller's principal.
        @throws NotImplementedError in the base class.
        """
        raise NotImplementedError

    def register_routes(self, app, prefix: str = "/auth") -> None:
        """!
        @brief Add any login/callback/logout routes this backend needs.
        @param app the FastAPI application.
        @param prefix URL prefix for the routes.
        """

    def describe(self) -> Dict[str, Any]:
        """!
        @brief Configuration summary for ``/api/status``, with no secrets.
        @return ``{"enabled": ..., "provider": ...}``.
        """
        return {"enabled": self.enabled, "provider": self.name}


class NoAuth(Authenticator):
    """!
    @brief Allows every request; the default, as Design.md specifies.
    """

    enabled = False
    name = "none"

    async def authenticate(self, request) -> Principal:
        """!
        @brief Always anonymous.
        @param request the Starlette request (ignored).
        @return ANONYMOUS.
        """
        return ANONYMOUS

    async def require_user(self, request) -> Principal:
        """!
        @brief Always admits the caller, as ANONYMOUS.

        Note ANONYMOUS has ``authenticated=False`` and no groups, so the
        SSO-group admin path in the API can never succeed under NoAuth.

        @param request the Starlette request (ignored).
        @return ANONYMOUS.
        """
        return ANONYMOUS


class OIDCAuthenticator(Authenticator):
    """!
    @brief OIDC authorization-code flow with a signed session cookie.

    OIDC authorization-code flow with a signed session cookie.

    Two credential styles are accepted, because both are needed in practice:
    a browser session cookie established by the login flow, and an
    ``Authorization: Bearer <id_token>`` header for scripted API clients that
    obtained a token elsewhere.

    @param config the ``auth`` configuration section.
    """

    enabled = True
    name = "oidc"

    def __init__(self, config: AuthConfig) -> None:
        """!
        @brief Store configuration; the OIDC client is built in startup().
        @param config the ``auth`` configuration section.
        """
        self.config = config
        self._oauth = None
        self._client = None
        # itsdangerous serializer that signs/verifies the session cookie.
        self._serializer = None
        self._metadata: Dict[str, Any] = {}

    # -- lifecycle ----------------------------------------------------------

    async def startup(self) -> None:
        """!
        @brief Import the optional dependencies, build the cookie signer and
        register the OIDC client.

        Discovery metadata is fetched lazily by authlib on first use, not
        here.

        @throws AuthError if authlib/itsdangerous are missing or
            ``auth.session_secret`` is unset.
        """
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

        # Config.validate() already requires this when auth is enabled;
        # checked again because an Authenticator can be built directly.
        secret = self.config.session_secret
        if not secret:
            raise AuthError(
                "auth.session_secret is unset; generate one with: "
                "openssl rand -hex 32"
            )
        # The salt namespaces these signatures, so a value signed with the
        # same secret for another purpose cannot be replayed as a session.
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
        """!
        @brief Map OIDC claims onto a Principal.

        Groups come from ``groups`` or, failing that, ``memberOf``; a single
        string is promoted to a one-element list.

        @param claims verified ID-token or userinfo claims.
        @return an authenticated Principal.
        """
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
        """!
        @brief Identify the caller from a bearer token or the session cookie.

        A bearer header, if present, is decisive: an invalid token yields
        ANONYMOUS without falling back to the cookie.

        @param request the Starlette request.
        @return the caller's principal, or ANONYMOUS.
        """
        header = request.headers.get("authorization", "")
        if header.lower().startswith("bearer "):
            # len("bearer ") == 7.
            token = header[7:].strip()
            claims = await self._verify_id_token(token)
            if claims is not None:
                return self._principal_from_claims(claims)
            return ANONYMOUS

        cookie = request.cookies.get(self.config.session_cookie)
        if cookie and self._serializer is not None:
            from itsdangerous import BadSignature, SignatureExpired
            try:
                # max_age is checked against the timestamp embedded at signing.
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
        """!
        @brief Validate a bearer ID token against the provider's JWKS.

        Checks signature (RS256/ES256), ``iss`` against the discovered issuer,
        ``aud`` against ``auth.client_id``, and the standard time claims via
        ``claims.validate()``.  Any failure is logged at debug and reported
        as None, never raised.

        @param token the compact JWT.
        @return the claims, or None if the token is not valid.
        """
        if self._client is None:
            return None
        try:
            metadata = await self._client.load_server_metadata()
            jwks = await self._client.fetch_jwk_set()
            from authlib.jose import JsonWebToken

            # Explicit algorithm allow-list: never accept "none" or HS*.
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
        """!
        @brief FastAPI dependency: demand an authenticated, authorised caller.
        @param request the Starlette request.
        @return the caller's principal.
        @throws HTTPException 401 when no valid credential is present; 403
            when ``auth.allowed_groups`` is set and the caller is in none.
        """
        from fastapi import HTTPException, status

        principal = await self.authenticate(request)
        if not principal.authenticated:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required; sign in at /auth/login",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Empty allowed_groups means any authenticated caller is admitted.
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
        """!
        @brief Add ``/auth/login``, ``/auth/callback``, ``/auth/logout`` and
        ``/auth/whoami``, all hidden from the OpenAPI schema.

        These rely on Starlette's SessionMiddleware (added by create_app when
        auth is enabled) for ``request.session``.

        @param app the FastAPI application.
        @param prefix URL prefix for the routes.
        """
        from fastapi import HTTPException, Request
        from fastapi.responses import JSONResponse, RedirectResponse

        @app.get(prefix + "/login", include_in_schema=False)
        async def login(request: Request, next: str = "/"):
            """!
            @brief Start the authorization-code flow.
            @param request the Starlette request.
            @param next where to send the browser after a successful sign-in.
            @return a redirect to the provider's authorization endpoint.
            @throws HTTPException 503 if startup() has not run.
            """
            if self._client is None:
                raise HTTPException(503, "OIDC is not initialised")
            # The nonce guards against token replay; it is carried in the
            # Starlette session and checked on the way back.
            nonce = secrets.token_urlsafe(16)
            request.session["oidc_nonce"] = nonce
            # NOTE: stored and later redirected to as given, without checking
            # that it is a same-origin path.
            request.session["post_login_redirect"] = next
            redirect_uri = self.config.redirect_url or str(
                request.url_for("auth_callback")
            )
            return await self._client.authorize_redirect(
                request, redirect_uri, nonce=nonce
            )

        @app.get(prefix + "/callback", name="auth_callback", include_in_schema=False)
        async def callback(request: Request):
            """!
            @brief Complete the flow: exchange the code, set the session cookie.
            @param request the Starlette request carrying ``code``/``state``.
            @return a 303 redirect to the saved post-login target.
            @throws HTTPException 503 if startup() has not run; 401 if the
                token exchange fails.
            """
            if self._client is None:
                raise HTTPException(503, "OIDC is not initialised")
            try:
                # authlib checks ``state`` against the session here.
                token = await self._client.authorize_access_token(request)
            except Exception as exc:
                log.warning("OIDC callback failed: %s", exc)
                raise HTTPException(401, "single sign-on failed: {}".format(exc))

            nonce = request.session.pop("oidc_nonce", None)
            # Recent authlib parses the ID token itself and exposes the claims
            # as ``userinfo``; otherwise parse it here, checking the nonce.
            claims = token.get("userinfo")
            if claims is None:
                claims = await self._client.parse_id_token(token, nonce=nonce)

            principal = self._principal_from_claims(dict(claims))
            target = request.session.pop("post_login_redirect", "/")
            response = RedirectResponse(url=target, status_code=303)
            # The whole claim set is signed into the cookie; there is no
            # server-side session store.  ``secure`` follows the scheme the
            # app sees, which is http behind a TLS-terminating proxy unless
            # proxy headers are honoured.
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
            """!
            @brief Drop the session cookie and the Starlette session.

            Local logout only; the provider session is not ended.

            @param request the Starlette request.
            @return a 303 redirect to ``/``.
            """
            response = RedirectResponse(url="/", status_code=303)
            response.delete_cookie(self.config.session_cookie)
            request.session.clear()
            return response

        @app.get(prefix + "/whoami", include_in_schema=False)
        async def whoami(request: Request):
            """!
            @brief Report the caller's principal (ANONYMOUS if not signed in).
            @param request the Starlette request.
            @return the principal as JSON.
            """
            principal = await self.authenticate(request)
            return JSONResponse(principal.as_dict())

    def describe(self) -> Dict[str, Any]:
        """!
        @brief Configuration summary for ``/api/status``, with no secrets.
        @return issuer, client id, scopes and allowed groups.
        """
        return {
            "enabled": True,
            "provider": self.name,
            "issuer": self.config.issuer,
            "client_id": self.config.client_id,
            "scopes": list(self.config.scopes),
            "allowed_groups": list(self.config.allowed_groups),
        }


def build_authenticator(config: AuthConfig) -> Authenticator:
    """!
    @brief Return the backend selected by *config*.
    @param config the ``auth`` configuration section.
    @return NoAuth when disabled, otherwise an OIDCAuthenticator.
    @throws AuthError for an unknown ``auth.provider``.
    """
    if not config.enabled:
        return NoAuth()
    # Several spellings accepted for the one supported provider.
    if config.provider.lower() in ("oidc", "openid", "fermilab", "fnal"):
        return OIDCAuthenticator(config)
    raise AuthError(
        "unknown auth.provider {!r}; supported: oidc".format(config.provider)
    )
