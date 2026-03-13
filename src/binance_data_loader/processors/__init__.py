"""Processor classes for different Binance data types."""

from binance_data_loader.processors.base import BaseProcessor
from binance_data_loader.processors.aggtrades import AggTradeProcessor
from binance_data_loader.processors.bookdepth import BookDepthProcessor
from binance_data_loader.processors.klines import KlineProcessor

__all__ = [
    "BaseProcessor",
    "AggTradeProcessor",
    "BookDepthProcessor",
    "KlineProcessor",
]
