import pytest
import httpx

from core.crawl_errors import ERROR_TYPE_KEY, HTTP_STATUS_KEY, error_context
from core.content_quality import crawl_failure_error_type


@pytest.mark.parametrize(("error", "expected"), [
    (TimeoutError("slow"), "Timeout"),
    (httpx.ConnectError("connection lost"), "NetworkError"),
    ("net::ERR_NAME_NOT_RESOLVED", "NetworkError"),
    ("Browser has been closed", "BrowserError"),
    ("HTTP 429", "HttpError:429"),
    (RuntimeError("unexpected"), "CrawlError"),
])
def test_failure_categories(error, expected):
    assert crawl_failure_error_type(error_context(error)) == expected


def test_structured_http_status_takes_precedence_over_message():
    error = httpx.HTTPStatusError(
        "arbitrary message", request=httpx.Request("GET", "https://example.com"),
        response=httpx.Response(503),
    )
    context = error_context(error)
    assert context == {ERROR_TYPE_KEY: "HttpError", HTTP_STATUS_KEY: 503}
    assert crawl_failure_error_type(context) == "HttpError:503"
