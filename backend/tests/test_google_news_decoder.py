import asyncio
from types import SimpleNamespace

import pytest

import adapters.retrieve_news_data.google_news_rss_searcher as module


URL = "https://news.google.com/rss/articles/test"
ARTICLE = "https://publisher.example/article"


@pytest.fixture(autouse=True)
def isolated_decoder(monkeypatch):
    monkeypatch.setattr(module, "_DECODE_CACHE", {})
    monkeypatch.setattr(module, "_DECODE_SEMAPHORE", asyncio.Semaphore(2))


def test_failed_decode_is_retried_and_only_success_is_cached(monkeypatch):
    calls = []

    async def decode(url):
        calls.append(url)
        return {"status": len(calls) > 1, "decoded_url": ARTICLE}

    monkeypatch.setattr(module, "_run_decoder", decode)

    async def scenario():
        assert await module._decode_google_news_url_async(URL) == URL
        assert URL not in module._DECODE_CACHE
        assert await module._decode_google_news_url_async(URL) == ARTICLE
        assert await module._decode_google_news_url_async(URL) == ARTICLE

    asyncio.run(scenario())
    assert calls == [URL, URL]


def test_deadline_kills_and_reaps_the_decoder_process(monkeypatch):
    class Process:
        returncode = None
        killed = False
        reaped = False

        async def communicate(self):
            await asyncio.Event().wait()

        def kill(self):
            self.killed = True

        async def wait(self):
            self.returncode = -9
            self.reaped = True

    process = Process()

    async def create(*args, **kwargs):
        return process

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(module, "_DECODE_TIMEOUT_SECONDS", 0.01)
    assert asyncio.run(module._decode_google_news_url_async(URL)) == URL
    assert process.killed and process.reaped
    assert URL not in module._DECODE_CACHE


def test_deadline_also_bounds_waiting_for_a_decoder_slot(monkeypatch):
    monkeypatch.setattr(module, "_DECODE_SEMAPHORE", asyncio.Semaphore(0))
    monkeypatch.setattr(module, "_DECODE_TIMEOUT_SECONDS", 0.01)
    assert asyncio.run(module._decode_google_news_url_async(URL)) == URL


def test_429_is_retried_once_with_backoff(monkeypatch):
    calls = []
    sleeps = []

    async def decode(url):
        calls.append(url)
        return {"status": False, "message": "HTTP 429"}

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(module, "_run_decoder", decode)
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    assert asyncio.run(module._decode_google_news_url_async(URL)) == URL
    assert len(calls) == 2 and sleeps == [5]


def test_sync_decode_has_a_timeout_and_does_not_cache_failure(monkeypatch):
    def run(*args, **kwargs):
        assert kwargs["timeout"] == module._DECODE_TIMEOUT_SECONDS
        return SimpleNamespace(stdout=b'{"status": false}')

    monkeypatch.setattr(module.subprocess, "run", run)
    assert module._decode_google_news_url(URL) == URL
    assert URL not in module._DECODE_CACHE


def test_decoder_subprocess_returns_json_without_network(monkeypatch):
    monkeypatch.setattr(module, "_DECODE_COMMAND", 'print(\'{"status": true, "decoded_url": "https://publisher.example/article"}\')')
    assert asyncio.run(module._decode_google_news_url_async(URL)) == ARTICLE


def test_cancellation_is_propagated_and_does_not_cache_failure(monkeypatch):
    async def cancelled(url):
        raise asyncio.CancelledError

    monkeypatch.setattr(module, "_run_decoder", cancelled)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(module._decode_google_news_url_async(URL))
    assert URL not in module._DECODE_CACHE
