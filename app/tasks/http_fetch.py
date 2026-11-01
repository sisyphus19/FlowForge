"""`http_fetch` - GET/POST a URL with SSRF guards, timeout and size limit."""
from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse

import httpx

from .registry import NonRetryableError, TaskContext, TaskError, register

MAX_BODY_BYTES = 5_000_000
_transport: httpx.BaseTransport | None = None  # tests inject httpx.MockTransport here


def _is_private(ip: str) -> bool:
    a = ipaddress.ip_address(ip)
    return a.is_private or a.is_loopback or a.is_link_local or a.is_reserved or a.is_multicast


def check_url(url: str, allowed_hosts: tuple[str, ...]) -> None:
    """Allow only http(s); if an allowlist is configured the host must be on it; otherwise block
    private/loopback targets. (Resolution-time checks do not fully stop DNS rebinding; production
    deployments should also use an egress proxy/firewall.)"""
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        raise NonRetryableError(f"unsupported URL: {url!r}")
    host = p.hostname.lower()
    if allowed_hosts:
        if host not in allowed_hosts:
            raise NonRetryableError(f"host {host!r} is not in FLOWFORGE_ALLOWED_HTTP_HOSTS")
        return
    if host == "localhost":
        raise NonRetryableError("requests to localhost are blocked")
    try:
        ips = [host] if _looks_like_ip(host) else [ai[4][0] for ai in socket.getaddrinfo(host, None)]
    except OSError as e:
        raise TaskError(f"cannot resolve {host}: {e}") from e
    if any(_is_private(ip) for ip in ips):
        raise NonRetryableError(f"host {host!r} resolves to a private address")


def _looks_like_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


@register("http_fetch")
def http_fetch(ctx: TaskContext):
    cfg = ctx.config
    url = cfg.get("url")
    if not url:
        raise NonRetryableError("http_fetch requires config.url")
    settings = ctx.services.get("settings")
    check_url(url, tuple(getattr(settings, "allowed_http_hosts", ()) or ()))
    method = str(cfg.get("method", "GET")).upper()
    if method not in ("GET", "POST"):
        raise NonRetryableError("method must be GET or POST")
    try:
        with httpx.Client(timeout=float(cfg.get("timeout_s", 10)), transport=_transport,
                          follow_redirects=False) as client:
            resp = client.request(method, url, headers=cfg.get("headers"), json=cfg.get("json"))
    except httpx.HTTPError as e:
        raise TaskError(f"HTTP error: {e}") from e
    if resp.status_code >= 500 or resp.status_code == 429:
        raise TaskError(f"upstream returned {resp.status_code}")
    if resp.status_code >= 400:
        raise NonRetryableError(f"upstream returned {resp.status_code}")
    if len(resp.content) > MAX_BODY_BYTES:
        raise NonRetryableError("response too large")
    try:
        data = resp.json()
    except ValueError:
        if cfg.get("expect_json", True):
            raise TaskError("response was not valid JSON") from None
        data = resp.text
    return {"status": resp.status_code, "data": data}
