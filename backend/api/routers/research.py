"""Natural-language research endpoints, including SSE chat streaming."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse

from agent.service import (
    InvalidResearchCitation,
    ResearchError,
    ResearchService,
    ResearchStreamEvent,
    ResearchUnavailable,
    ResearchUpstreamError,
)
from api.deps import get_research_service
from api.schemas import (
    ResearchCitation,
    ResearchRequest,
    ResearchResponse,
    ResearchStreamRequest,
    SourceRef,
)
from core.logger import get_logger


router = APIRouter(prefix="/api/research", tags=["research"])
logger = get_logger(__name__)
SSE_HEARTBEAT_SECONDS = 15


def _http_error(exc: ResearchError, status_code: int) -> HTTPException:
    headers = {"X-Request-ID": exc.request_id} if exc.request_id else None
    return HTTPException(
        status_code=status_code,
        detail=exc.public_detail(),
        headers=headers,
    )


def _encode_sse(event: ResearchStreamEvent) -> bytes:
    payload = json.dumps(
        jsonable_encoder(event.data),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"event: {event.event}\ndata: {payload}\n\n".encode()


async def _stream_events(session) -> AsyncIterator[bytes]:
    queue: asyncio.Queue[tuple[str, object | None]] = asyncio.Queue()

    async def produce() -> None:
        try:
            async for event in session.events:
                await queue.put(("event", event))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await queue.put(("error", exc))
        finally:
            await queue.put(("end", None))

    producer = asyncio.create_task(produce())
    try:
        while True:
            try:
                kind, payload = await asyncio.wait_for(
                    queue.get(), timeout=SSE_HEARTBEAT_SECONDS
                )
            except TimeoutError:
                yield b": keep-alive\n\n"
                continue
            if kind == "event":
                if isinstance(payload, ResearchStreamEvent):
                    yield _encode_sse(payload)
                continue
            if kind == "error":
                exc = payload
                if isinstance(exc, InvalidResearchCitation):
                    yield _encode_sse(ResearchStreamEvent("reset", {}))
                if isinstance(exc, ResearchError):
                    detail = exc.public_detail()
                else:
                    logger.exception(
                        "Unexpected research stream failure request_id=%s",
                        session.request_id,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
                    detail = {
                        "message": "Research stream failed",
                        "error_type": type(exc).__name__,
                        "reason": "Unexpected streaming failure; inspect API logs",
                        "request_id": session.request_id,
                    }
                yield _encode_sse(ResearchStreamEvent("error", detail))
                break
            if kind == "end":
                break
    finally:
        if not producer.done():
            producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)


@router.post("", response_model=ResearchResponse)
async def research(
    request: ResearchRequest,
    response: Response,
    service: ResearchService = Depends(get_research_service),
) -> ResearchResponse:
    try:
        result = await service.research(request.query)
    except ResearchUnavailable as exc:
        raise _http_error(exc, 503) from exc
    except (ResearchUpstreamError, InvalidResearchCitation) as exc:
        raise _http_error(exc, 502) from exc
    if result is None:
        request_id = uuid4().hex
        reason = "Research service returned None instead of ResearchResult"
        logger.error(
            "Research endpoint failed request_id=%s error_type=EmptyServiceResult "
            "reason=%s",
            request_id,
            reason,
        )
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Research service returned no result",
                "error_type": "EmptyServiceResult",
                "reason": reason,
                "request_id": request_id,
            },
            headers={"X-Request-ID": request_id},
        )
    if result.request_id:
        response.headers["X-Request-ID"] = result.request_id
    return ResearchResponse(
        answer=result.answer,
        citations=[
            ResearchCitation(
                article_id=citation.article_id,
                title=citation.title,
                url=citation.url,
                source=(
                    SourceRef(**citation.source) if citation.source is not None else None
                ),
                published_at=citation.published_at,
            )
            for citation in result.citations
        ],
    )


@router.post("/stream")
async def stream_research(
    request: ResearchStreamRequest,
    service: ResearchService = Depends(get_research_service),
) -> StreamingResponse:
    try:
        session = service.start_stream(
            request.query,
            thread_id=str(request.thread_id),
        )
    except ResearchUnavailable as exc:
        raise _http_error(exc, 503) from exc
    except (ResearchUpstreamError, InvalidResearchCitation) as exc:
        raise _http_error(exc, 502) from exc
    return StreamingResponse(
        _stream_events(session),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "X-Request-ID": session.request_id,
        },
    )


@router.delete("/threads/{thread_id}", status_code=204)
async def delete_research_thread(
    thread_id: UUID,
    service: ResearchService = Depends(get_research_service),
) -> Response:
    await service.delete_thread(str(thread_id))
    return Response(status_code=204)
