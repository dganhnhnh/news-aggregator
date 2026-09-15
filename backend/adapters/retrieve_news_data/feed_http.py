"""Bounded, redirect-safe HTTP reads for feed discovery.

Discovery follows URLs supplied by search engines and arbitrary publisher
pages.  Unlike configured crawl targets, those URLs are not trusted operator
input, so every hop must remain on the public internet and every response body
must be bounded.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import ipaddress
import socket
from urllib.parse import urljoin, urlsplit

import httpx

from core.feed_urls import validate_feed_url


MAX_DISCOVERY_BODY_BYTES = 1_000_000
MAX_DISCOVERY_REDIRECTS = 5


class UnsafeFeedUrl(ValueError):
    """Raised when discovery would fetch a non-public URL."""


@dataclass(frozen=True)
class DiscoveryHttpResponse:
    url: str
    body: bytes
    content_type: str


def validate_public_url(value: str) -> str:
    """Structurally validate a URL and reject obvious local-network targets."""
    validated = validate_feed_url(value)
    parts = urlsplit(validated)
    if parts.username or parts.password:
        raise UnsafeFeedUrl(f"Discovery URL must not contain credentials: {value!r}")
    hostname = (parts.hostname or "").casefold().rstrip(".")
    if (
        hostname == "localhost"
        or hostname.endswith(".localhost")
        or hostname.endswith(".local")
    ):
        raise UnsafeFeedUrl(f"Discovery URL is not public: {value!r}")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return validated
    if not address.is_global:
        raise UnsafeFeedUrl(f"Discovery URL is not public: {value!r}")
    return validated


async def ensure_public_url(value: str, *, resolve_dns: bool) -> str:
    """Validate a URL and optionally reject hostnames resolving privately."""
    validated = validate_public_url(value)
    if not resolve_dns:
        return validated
    parts = urlsplit(validated)
    port = parts.port or (443 if parts.scheme.casefold() == "https" else 80)
    try:
        addresses = await asyncio.to_thread(
            socket.getaddrinfo,
            parts.hostname,
            port,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise UnsafeFeedUrl(f"Discovery host could not be resolved: {parts.hostname}") from exc
    if not addresses:
        raise UnsafeFeedUrl(f"Discovery host could not be resolved: {parts.hostname}")
    for address_info in addresses:
        address = ipaddress.ip_address(address_info[4][0])
        if not address.is_global:
            raise UnsafeFeedUrl(
                f"Discovery host resolved to a non-public address: {parts.hostname}"
            )
    return validated


async def fetch_discovery_url(
    client: httpx.AsyncClient,
    url: str,
    *,
    timeout_seconds: float,
    user_agent: str,
    resolve_dns: bool,
) -> DiscoveryHttpResponse:
    """Fetch at most one bounded body while validating every redirect hop."""
    current = url
    for redirect_count in range(MAX_DISCOVERY_REDIRECTS + 1):
        current = await ensure_public_url(current, resolve_dns=resolve_dns)
        async with client.stream(
            "GET",
            current,
            headers={"User-Agent": user_agent},
            timeout=timeout_seconds,
            follow_redirects=False,
        ) as response:
            if response.is_redirect:
                location = response.headers.get("location", "").strip()
                if not location:
                    response.raise_for_status()
                if redirect_count >= MAX_DISCOVERY_REDIRECTS:
                    raise httpx.TooManyRedirects(
                        "Too many redirects during feed discovery",
                        request=response.request,
                    )
                current = urljoin(str(response.url), location)
                continue

            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                remaining = MAX_DISCOVERY_BODY_BYTES - len(body)
                if remaining <= 0:
                    break
                body.extend(chunk[:remaining])
                if len(body) >= MAX_DISCOVERY_BODY_BYTES:
                    break
            return DiscoveryHttpResponse(
                url=await ensure_public_url(
                    str(response.url), resolve_dns=resolve_dns
                ),
                body=bytes(body),
                content_type=response.headers.get("content-type", ""),
            )
    raise AssertionError("redirect loop did not terminate")
