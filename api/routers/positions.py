# api/routers/positions.py

from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_positions():
    """Placeholder for position endpoints"""
    return {"message": "Position endpoints coming soon"}