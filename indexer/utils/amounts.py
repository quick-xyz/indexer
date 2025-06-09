# indexer/utils/amounts.py
"""
Utility functions for handling string amounts in blockchain operations
"""

from typing import Union, Iterable


def amount_to_int(amount: Union[str, int, None]) -> int:
    """Convert amount to int with robust type handling"""
    if amount is None:
        return 0
    if isinstance(amount, str):
        if amount.strip() == "":
            return 0
        return int(amount)
    if isinstance(amount, (int, float)):
        return int(amount)
    # Handle any other numeric types
    try:
        return int(amount)
    except (ValueError, TypeError):
        return 0


def amount_to_str(amount: Union[str, int, None]) -> str:
    """Convert amount to string with robust type handling"""
    if amount is None:
        return "0"
    if isinstance(amount, str):
        if amount.strip() == "":
            return "0"
        try:
            int(amount)
            return amount
        except ValueError:
            return "0"
    return str(amount)


def add_amounts(amounts: Iterable[Union[str, int]]) -> str:
    """Add multiple amounts and return as string"""
    total = sum(amount_to_int(amt) for amt in amounts)
    return str(total)


def subtract_amounts(amount1: Union[str, int], amount2: Union[str, int]) -> str:
    """Subtract amount2 from amount1 and return as string"""
    result = amount_to_int(amount1) - amount_to_int(amount2)
    return str(result)


def compare_amounts(amount1: Union[str, int], amount2: Union[str, int]) -> int:
    """
    Compare two amounts.
    Returns: -1 if amount1 < amount2, 0 if equal, 1 if amount1 > amount2
    """
    int1, int2 = amount_to_int(amount1), amount_to_int(amount2)
    if int1 < int2:
        return -1
    elif int1 > int2:
        return 1
    else:
        return 0


def is_positive(amount: Union[str, int]) -> bool:
    """Check if amount is positive"""
    return amount_to_int(amount) > 0


def is_zero(amount: Union[str, int]) -> bool:
    """Check if amount is zero"""
    return amount_to_int(amount) == 0


def abs_amount(amount: Union[str, int]) -> str:
    """Return absolute value as string"""
    return str(abs(amount_to_int(amount)))


def multiply_amount(amount: Union[str, int], multiplier: Union[str, int, float]) -> str:
    """Multiply amount by multiplier and return as string"""
    if isinstance(multiplier, float):
        # Handle decimal multipliers carefully
        result = int(amount_to_int(amount) * multiplier)
    else:
        result = amount_to_int(amount) * amount_to_int(multiplier)
    return str(result)