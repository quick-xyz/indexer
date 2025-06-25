# api/routers/liquidity.py

from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_liquidity():
    """Placeholder for liquidity endpoints"""
    return {"message": "Liquidity endpoints coming soon"}