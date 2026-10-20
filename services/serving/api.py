"""FastAPI application. All business HTTP methods are read-only."""
from contextlib import asynccontextmanager
import logging
import os
from typing import Annotated

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .contracts import ServingError, utc
from .repository import Repository

RunID = Annotated[str, Query(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")]
BatchID = Annotated[int | None, Query(ge=0)]
Limit = Annotated[int, Query(ge=1, le=200)]


def create_app(database=None):
    @asynccontextmanager
    async def lifespan(app):
        if database is None:
            from pymongo import MongoClient
            client = MongoClient(os.getenv("MONGO_HOST", "127.0.0.1"), port=int(os.getenv("MONGO_PORT", "27018")),
                username=os.environ["MONGO_USERNAME"], password=os.environ["MONGO_PASSWORD"],
                authSource=os.environ["MONGO_DATABASE"], serverSelectionTimeoutMS=2000,
                connectTimeoutMS=2000, socketTimeoutMS=3000, maxPoolSize=8,
                appname="ecommerce-read-api")
            app.state.repository = Repository(client[os.environ["MONGO_DATABASE"]])
        else:
            client = None
            app.state.repository = Repository(database)
        try:
            yield
        finally:
            if client is not None: client.close()

    app = FastAPI(title="E-commerce Analytics API", version="1.0.0", lifespan=lifespan)

    @app.middleware("http")
    async def no_cache(request, call_next):
        try:
            response = await call_next(request)
        except Exception as error:
            logging.getLogger(__name__).error("Serving backend failure: %s", type(error).__name__)
            response = JSONResponse(status_code=503, content={"detail": {"code": "BACKEND_UNAVAILABLE", "message": "Không đọc được dữ liệu đã công bố"}})
        response.headers["Cache-Control"] = "no-store"
        return response

    app.add_middleware(CORSMiddleware, allow_origin_regex=r"http://(?:localhost|127\.0\.0\.1)(?::[0-9]{1,5})?",
                       allow_methods=["GET"], allow_headers=["Accept", "Content-Type"], allow_credentials=False)

    @app.exception_handler(ServingError)
    async def serving_error(request, error):
        return JSONResponse(status_code=error.status, content={"detail": {"code": error.code, "message": error.message}})

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        return JSONResponse(status_code=422, content={"detail": {"code": "INVALID_ARGUMENT", "message": "Tham số không hợp lệ"}})

    @app.exception_handler(Exception)
    async def backend_error(request, error):
        # Neither credentials, request query strings nor backend exception text enter the response/log.
        logging.getLogger(__name__).error("Serving backend failure: %s", type(error).__name__)
        return JSONResponse(status_code=503, content={"detail": {"code": "BACKEND_UNAVAILABLE", "message": "Không đọc được dữ liệu đã công bố"}})

    def repository(request: Request):
        return request.app.state.repository

    Repo = Annotated[Repository, Depends(repository)]

    def context(repo: Repo, run_id: RunID, batch_id: BatchID = None):
        return repo, repo.context(run_id, batch_id)

    Ctx = Annotated[tuple, Depends(context)]

    @app.get("/api/v1/health")
    def health(repo: Repo):
        repo.db.command("ping")
        return {"status": "ready", "mongodb": True}

    @app.get("/api/v1/runs")
    def runs(repo: Repo, limit: Limit = 50, after: str | None = None):
        return repo.runs(limit, after)

    @app.get("/api/v1/pipeline")
    def pipeline(ctx: Ctx):
        return ctx[0].pipeline(ctx[1])

    @app.get("/api/v1/overview")
    def overview(ctx: Ctx):
        return ctx[0].overview(ctx[1])

    @app.get("/api/v1/timeseries")
    def timeseries(ctx: Ctx, start: str | None = None, end: str | None = None):
        start, end = utc(start) if start else None, utc(end) if end else None
        if start and end and start >= end:
            raise ServingError(422, "INVALID_TIME_RANGE", "START phải trước END")
        return ctx[0].timeseries(ctx[1], start, end)

    @app.get("/api/v1/funnel")
    def funnel(ctx: Ctx, limit: Limit = 20):
        return ctx[0].funnel(ctx[1], limit)

    @app.get("/api/v1/segments")
    def segments(ctx: Ctx):
        return ctx[0].segments(ctx[1])

    @app.get("/api/v1/predictions")
    def predictions(ctx: Ctx, limit: Limit = 50, after: str | None = None,
                    cluster_id: Annotated[int | None, Query(ge=0)] = None,
                    predicted_label: Annotated[int | None, Query(ge=0, le=1)] = None):
        return ctx[0].predictions(ctx[1], limit, after, cluster_id, predicted_label)

    @app.get("/api/v1/visitors/{visitor_id}")
    def visitor(ctx: Ctx, visitor_id: str):
        if not visitor_id.isascii() or not visitor_id.isdigit() or len(visitor_id) > 128:
            raise ServingError(422, "INVALID_VISITOR", "Visitor ID phải là chuỗi chữ số")
        return ctx[0].visitor(ctx[1], visitor_id)

    @app.get("/api/v1/models")
    def models(ctx: Ctx):
        return ctx[0].models(ctx[1])

    return app


app = create_app()
