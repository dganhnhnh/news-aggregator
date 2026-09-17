"""FastAPI entry point for private operator mutations."""

from __future__ import annotations

from fastapi import FastAPI

from admin_api.deps import get_settings, lifespan
from admin_api.routers import feeds, metrics
from core.logger import setup_logging
from observability import HttpMetricsMiddleware


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level)
    app = FastAPI(title="News Radar Admin API", lifespan=lifespan)
    app.add_middleware(HttpMetricsMiddleware)
    app.include_router(feeds.router)
    app.include_router(metrics.router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "admin_api.main:app",
        host=settings.admin_api_host,
        port=settings.admin_api_port,
        reload=False,
    )


if __name__ == "__main__":
    run()
