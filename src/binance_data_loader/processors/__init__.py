"""Processor classes for different Binance data types."""

from binance_data_loader.processors.base import BaseProcessor
from binance_data_loader.processors.klines import KlineProcessor
from binance_data_loader.processors.aggtrades import AggTradeProcessor

__all__ = ["BaseProcessor", "KlineProcessor", "AggTradeProcessor"]
