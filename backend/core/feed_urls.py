"""Validation and display-label policy for operator-managed feeds."""

from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit

import tldextract

from core.urls import UnusableUrl, resolve_crawl_url


# Use tldextract's bundled Public Suffix List snapshot. Admin requests must not
# depend on a network refresh or write a cache file on first use.
_extract_domain = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


def validate_feed_url(value: str) -> str:
    """Return a trimmed absolute HTTP(S) feed URL without fetching it."""
    candidate = value.strip()
    try:
        return resolve_crawl_url(candidate)
    except UnusableUrl as exc:
        raise ValueError(str(exc)) from exc


def infer_feed_source_label(feed_url: str) -> str:
    """Infer the registrable-domain label, excluding subdomain and suffix."""
    validated = validate_feed_url(feed_url)
    hostname = (urlsplit(validated).hostname or "").lower().rstrip(".")
    if not hostname:
        raise ValueError(f"Invalid feed URL: {feed_url!r}")

    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        extracted = _extract_domain(hostname)
        return extracted.domain or hostname
    return hostname
