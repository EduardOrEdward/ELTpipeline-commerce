
#from typing import Optional
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse
from mock_api.routers import orders, deliveries, inventory
import logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app:FastAPI):
    pass








app = FastAPI(title="Mock_API",debug=True,summary="Our mock_api with synthetic data",lifespan=lifespan)
app.include_router(router=orders.router)
app.include_router(router=deliveries.router)
app.include_router(router=inventory.router)
