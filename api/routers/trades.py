# api/routers/trades.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any
import logging

from indexer.database.repository_manager import RepositoryManager
from indexer.core.logging import log_with_context
from ..dependencies import get_repository_manager, get_database_session, get_logger

router = APIRouter()

def format_trade(trade) -> Dict[str, Any]:
    """Convert trade model to API response format"""
    return {
        "id": str(trade.id),
        "taker": trade.taker,
        "direction": trade.direction.value,
        "base_token": trade.base_token,
        "base_amount": str(trade.base_amount),
        "quote_token": trade.quote_token,
        "quote_amount": str(trade.quote_amount) if trade.quote_amount else None,
        "trade_type": trade.trade_type.value,
        "timestamp": trade.timestamp.isoformat(),
        "block_number": trade.block_number,
        "tx_hash": trade.tx_hash,
        "swap_count": trade.swap_count,
        "transfer_count": trade.transfer_count
    }

@router.get("/recent")
async def get_recent_trades(
    limit: int = Query(default=100, le=1000, description="Number of trades to return (max 1000)"),
    repo_manager: RepositoryManager = Depends(get_repository_manager),
    session: Session = Depends(get_database_session),
    logger = Depends(get_logger)
):
    """Get recent trades"""
    try:
        trades = repo_manager.trade_repository.get_recent(session, limit=limit)
        
        trade_data = [format_trade(trade) for trade in trades]
        
        log_with_context(logger, logging.DEBUG, "Recent trades fetched",
                        count=len(trade_data), limit=limit)
        
        return {
            "trades": trade_data,
            "count": len(trade_data),
            "limit": limit
        }
        
    except Exception as e:
        log_with_context(logger, logging.ERROR, "Error fetching recent trades",
                        error=str(e), limit=limit)
        raise HTTPException(status_code=500, detail="Failed to fetch trades")

@router.get("/by-taker/{taker_address}")
async def get_trades_by_taker(
    taker_address: str,
    limit: int = Query(default=100, le=1000, description="Number of trades to return (max 1000)"),
    repo_manager: RepositoryManager = Depends(get_repository_manager),
    session: Session = Depends(get_database_session),
    logger = Depends(get_logger)
):
    """Get trades by taker address"""
    try:
        trades = repo_manager.trade_repository.get_by_taker(session, taker_address, limit=limit)
        
        trade_data = [format_trade(trade) for trade in trades]
        
        log_with_context(logger, logging.DEBUG, "Trades by taker fetched",
                        taker=taker_address, count=len(trade_data), limit=limit)
        
        return {
            "trades": trade_data,
            "taker": taker_address,
            "count": len(trade_data),
            "limit": limit
        }
        
    except Exception as e:
        log_with_context(logger, logging.ERROR, "Error fetching trades by taker",
                        taker=taker_address, error=str(e), limit=limit)
        raise HTTPException(status_code=500, detail="Failed to fetch trades")

@router.get("/by-token/{token_address}")
async def get_trades_by_token(
    token_address: str,
    limit: int = Query(default=100, le=1000, description="Number of trades to return (max 1000)"),
    repo_manager: RepositoryManager = Depends(get_repository_manager),
    session: Session = Depends(get_database_session),
    logger = Depends(get_logger)
):
    """Get trades involving a specific token"""
    try:
        trades = repo_manager.trade_repository.get_by_token(session, token_address, limit=limit)
        
        trade_data = [format_trade(trade) for trade in trades]
        
        log_with_context(logger, logging.DEBUG, "Trades by token fetched",
                        token=token_address, count=len(trade_data), limit=limit)
        
        return {
            "trades": trade_data,
            "token": token_address,
            "count": len(trade_data),
            "limit": limit
        }
        
    except Exception as e:
        log_with_context(logger, logging.ERROR, "Error fetching trades by token",
                        token=token_address, error=str(e), limit=limit)
        raise HTTPException(status_code=500, detail="Failed to fetch trades")

@router.get("/arbitrage")
async def get_arbitrage_trades(
    limit: int = Query(default=100, le=1000, description="Number of trades to return (max 1000)"),
    repo_manager: RepositoryManager = Depends(get_repository_manager),
    session: Session = Depends(get_database_session),
    logger = Depends(get_logger)
):
    """Get arbitrage trades only"""
    try:
        trades = repo_manager.trade_repository.get_arbitrage_trades(session, limit=limit)
        
        trade_data = [format_trade(trade) for trade in trades]
        
        log_with_context(logger, logging.DEBUG, "Arbitrage trades fetched",
                        count=len(trade_data), limit=limit)
        
        return {
            "trades": trade_data,
            "trade_type": "arbitrage",
            "count": len(trade_data),
            "limit": limit
        }
        
    except Exception as e:
        log_with_context(logger, logging.ERROR, "Error fetching arbitrage trades",
                        error=str(e), limit=limit)
        raise HTTPException(status_code=500, detail="Failed to fetch trades")