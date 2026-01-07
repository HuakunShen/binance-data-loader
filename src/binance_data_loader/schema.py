"""Pandera schemas for validating Binance data."""

import polars as pl
import pandera.polars as pa_polars


class BinanceKlineDataSchema(pa_polars.DataFrameModel):
    """
    Schema for Binance kline data validation.

    Note: We use milliseconds to avoid overflow issues with large nanosecond values.
    """

    open_time: pl.Datetime = pa_polars.Field(coerce=True)
    open: float = pa_polars.Field()
    high: float = pa_polars.Field()
    low: float = pa_polars.Field()
    close: float = pa_polars.Field()
    volume: float = pa_polars.Field()
    close_time: pl.Datetime = pa_polars.Field(coerce=True)
    quote_volume: float = pa_polars.Field()
    count: int = pa_polars.Field()
    taker_buy_volume: float = pa_polars.Field()
    taker_buy_quote_volume: float = pa_polars.Field()
    ignore: int = pa_polars.Field()


# Column names for Binance kline data (no header in CSV files)
BINANCE_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]
