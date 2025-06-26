# src\tubuin_processor\core\stats\types.py
"""
This module defines shared types and data structures used across the stats package,
preventing circular dependencies.
"""
from dataclasses import dataclass
from typing import Dict, Callable
import polars as pl

@dataclass(frozen=True)
class Stat:
    """
    Defines a computable statistic, including its implementation, description,
    and default execution status.
    """
    func: Callable[[Dict[str, pl.DataFrame]], pl.DataFrame]
    description: str
    default_enabled: bool = False