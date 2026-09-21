"""FastAPI application entry point for the `news-api` command."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.deps import get_settings, lifespan
from api.routers import articles, health, research, sources, stories, topics, trends
from core.logger import setup_logging
from observability import HttpMetricsMiddleware, metrics_asgi_app


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level)
    app = FastAPI(title="News Radar API", lifespan=lifespan)
    if settings.cors_allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_allow_origins),
            allow_methods=["*"],
            allow_headers=["*"],
        )
    # Added last, so it ends up outermost: what a client waits for is the
    # whole stack, CORS and error handling included.
    app.add_middleware(HttpMetricsMiddleware)
    app.include_router(health.router)
    app.include_router(articles.router)
    app.include_router(sources.router)
    app.include_router(stories.router)
    app.include_router(topics.router)
    app.include_router(trends.router)
    app.include_router(research.router)
    return app


app = create_app()
# Served on its own listener, never on the public one. Request rates, model
# spend and search latency are operator data in the same way queue depth is,
# and compose publishes this port on loopback alone.
metrics_app = metrics_asgi_app()


def run() -> None:
    import asyncio

    import uvicorn

    settings = get_settings()

    class ExporterServer(uvicorn.Server):
        """Runs beside the API, which owns the process signal handlers.

        Two servers installing their own handlers would leave SIGTERM
        stopping whichever registered last, so the container would sit
        through its stop timeout on every deploy.
        """

        def install_signal_handlers(self) -> None:
            return

    api = uvicorn.Server(
        uvicorn.Config(
            "api.main:app",
            host=settings.api_host,
            port=settings.api_port,
            reload=False,
        )
    )
    exporter = ExporterServer(
        uvicorn.Config(
            metrics_app,
            host=settings.api_metrics_host,
            port=settings.api_metrics_port,
            access_log=False,
        )
    )

    async def serve() -> None:
        exporting = asyncio.create_task(exporter.serve())
        try:
            await api.serve()
        finally:
            exporter.should_exit = True
            await exporting

    asyncio.run(serve())


if __name__ == "__main__":
    run()
