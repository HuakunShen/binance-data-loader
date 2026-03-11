"""Backward compatibility shim.

The original DataProcessor class has been superseded by the processor-first
architecture. Use processors.KlineProcessor instead.

This module is kept so that any existing code importing DataProcessor
continues to work without changes.
"""

from binance_data_loader.processors.klines import KlineProcessor as DataProcessor

__all__ = ["DataProcessor"]
