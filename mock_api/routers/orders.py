## ORDERS ENDPOINT
# WE NEED TO CALL THE FUNCTION GENERATOR TO MAKE AN ENDPOINT FOR AIRFLOW
from fastapi import APIRouter
import logging,time
logger = logging.getLogger(__name__)
from dateutil.relativedelta import relativedelta
router = APIRouter(tags=["Orders"])



