from contextlib import asynccontextmanager
import logging
import os
import random

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from mock_api.routers import orders, deliveries, inventory

logger = logging.getLogger(__name__)

# Small amount of controlled instability to make the mock API behave more like
# a real external service. /health is intentionally excluded so Docker can
# still distinguish an unhealthy container from a temporarily failing request.
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.05"))
if not 0 <= FAILURE_RATE <= 1:
    raise ValueError("FAILURE_RATE must be between 0 and 1")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="Mock_API",
    debug=True,
    summary="Our mock_api with synthetic data",
    lifespan=lifespan,
)


@app.middleware("http")
async def simulate_api_failures(request: Request, call_next):
    if request.url.path != "/health" and random.random() < FAILURE_RATE:
        logger.warning("Simulated upstream failure for %s", request.url.path)
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Simulated temporary upstream failure",
                "error_type": "temporary_upstream_error",
            },
        )

    return await call_next(request)


@app.get("/health")
async def health():
    return {"status": "ok"}


app.include_router(router=orders.router)
app.include_router(router=deliveries.router)
app.include_router(router=inventory.router)
