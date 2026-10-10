from fastapi import APIRouter

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health():
    """Report that the MockAPI process is responsive."""
    return {"status": "ok"}
