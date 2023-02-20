import pandas as pd
import numpy as np
from src.code.data.features.base import BaseFeatureGenerator
import os
from src.code.data.download.fmp_python.fmp import *
from src.code.utils.utils import create_date_chunks
from tqdm import tqdm
from multiprocessing import Pool
import time


class PriceActionFeatureGenerator(BaseFeatureGenerator):

    def __init__(self,
                 format_: str,
                 interval: str,
                 close_column_name: str = 'close'):
        BaseFeatureGenerator.__init__(self, format_=format_, interval=interval)

        self.close_column_name = close_column_name

    def calculate_candle_features(self, ticker: str, latest_price_data: pd.DataFrame):

        """
        Calculates the candle features like

        candle body = (close - open)
        upper_wick = high - close, if close > open
                     high - open, if open > close
        lower_wick = open - low, if close > open
                     close - low, if open > close

        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step
                                  sorted in descending order by the date column
        :return: Candle features - candle body, upper wick and lower wick sizes
        """

        ticker_latest_info = latest_price_data[latest_price_data["ticker"] == ticker].head(1)

        close = ticker_latest_info[self.close_column_name].item()
        open_ = ticker_latest_info["open"].item()
        high = ticker_latest_info["high"].item()
        low = ticker_latest_info["low"].item()

        candle_body = close - open_
        upper_wick = (high - close) if close > open_ else (high - open_)
        lower_wick = (open_ - low) if close > open_ else (close - low)
        candle_size = high - low

        return candle_size, candle_body, upper_wick, lower_wick

    def calculate_bar_composition(self, ticker: str, latest_price_data: pd.DataFrame):

        """
        Calculates the bar composition based on the upper wick, lower wick and the candle body

        upper wick composition = upper wick size/ total candle size
        lower wick composition = lower wick size/ total candle size
        body composition = body size/ total candle size

        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step
                                  sorted in descending order by the date column
        :return: Bar composition based on the above calculation
        """

        candle_size, candle_body, upper_wick, lower_wick = self.calculate_candle_features(ticker, latest_price_data)

        upper_wick_composition = upper_wick/candle_size
        lower_wick_composition = lower_wick/candle_size
        body_composition = candle_body/candle_size

        return body_composition, upper_wick_composition, lower_wick_composition

    def calculate_pivot_range(self,
                              ticker: str,
                              latest_price_data: pd.DataFrame,
                              prev_day_price: pd.DataFrame,
                              type: str = "standard",
                              close_price_column: str = "adjClose"):

        """
        Calculates pivot range for the time stamp. Pivot ranges do not change across the day, hence it has
        to be calculated only once in the beginning of the day.

        https://www.tradingview.com/support/solutions/43000521824-pivot-points-standard/

        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step
                                  sorted in descending order by the date column
        :param prev_day_price: Daily price data of the previous day. Will be a list with many ticker data
        :param type: Standard, Fibonacci etc.
        :param close_price_column: Adjusted close or close price column to use
        :return: pivot ranges for the timestamp
        """

        prev_day_ticker_data = prev_day_price[prev_day_price["ticker"] == ticker]

        prev_day_close = prev_day_ticker_data[close_price_column].item()
        prev_day_open = prev_day_ticker_data["open"].item()
        prev_day_high = prev_day_ticker_data["high"].item()
        prev_day_low = prev_day_ticker_data["low"].item()

        if type == "standard":
            pp = (prev_day_high + prev_day_low + prev_day_close)/3
            r1 = pp * 2 - prev_day_low
            s1 = pp * 2 - prev_day_high
            r2 = pp + (prev_day_high - prev_day_low)
            s2 = pp - (prev_day_high - prev_day_low)
            r3 = pp * 2 + (prev_day_high - 2 * prev_day_low)
            s3 = pp * 2 - (2 * prev_day_high - prev_day_low)
            r4 = pp * 3 + (prev_day_high - 3 * prev_day_low)
            s4 = pp * 3 - (3 * prev_day_high - prev_day_low)
            return pp, [r1, r2, r3, r4], [s1, s2, s3, s4]
        else:
            print("Have not implemented this type for now. Please check later")
            return None

    def calculate_price_ma_deviation(self, ticker: str,
                                     latest_price_data: pd.DataFrame,
                                     ma_type: int = 20):

        """
        Calculates the % deviation between the price and the moving average

        deviation = (price - moving average)/moving_average

        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step
                                  sorted in descending order by the date column
        :param ma_type: Moving average type - For eg. 20,50, 200 etc
        :return: deviation of close and open price witht he moving average
        """

        ticker_latest_info = latest_price_data[latest_price_data["ticker"] == ticker].head(1)
        ma = self.calculate_sma(ticker, latest_price_data, ma_type)
        close = ticker_latest_info[self.close_column_name].item()
        open_ = ticker_latest_info["open"].item()

        return (close - ma)/ma, (open_ - ma)/ma








