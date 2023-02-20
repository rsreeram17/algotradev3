import pandas as pd
import numpy as np
from src.code.data.features.base import BaseFeatureGenerator
import os
from src.code.data.download.fmp_python.fmp import *
from src.code.utils.utils import create_date_chunks
from tqdm import tqdm
from multiprocessing import Pool
import time


class TechnicalFeatureGenerator(BaseFeatureGenerator):

    def __init__(self,
                 format_: str,
                 interval: str,
                 close_column_name: str = 'close'):
        BaseFeatureGenerator.__init__(self,
                                      format_=format_,
                                      interval=interval,
                                      close_column_name=close_column_name)

    def calculate_atr(self, ticker: str, latest_price_data: pd.DataFrame, window: int = 14):

        """
        Calculate Average true range for the latest time step

        https://www.investopedia.com/terms/a/atr.asp

        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step
                                  sorted in descending order by the date column
        :param window: Window to calculate ATR (default - 14)
        :return: ATR for the latest timestep
        """

        ticker_latest_info = latest_price_data[latest_price_data["ticker"] == ticker]
        high_low = ticker_latest_info["high"] - ticker_latest_info["low"]
        high_close = np.abs(ticker_latest_info["high"] - ticker_latest_info[self.close_column_name].shift(-1))
        low_close = np.abs(ticker_latest_info["low"] - ticker_latest_info[self.close_column_name].shift(-1))
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges.head(window), axis=1)

        return np.mean(true_range)

    def volatility(self, ticker: str, latest_price_data: pd.DataFrame, window: int = 14):

        """
        Calculate the volatility of price by using standard deviation
        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step
                                  sorted in descending order by the date column
        :param window: Window of price to be used for the calculation
        :return: volatility number
        """

        ticker_latest_info = latest_price_data[latest_price_data["ticker"] == ticker]
        close_price = np.array(ticker_latest_info.head(window)[self.close_column_name])
        return np.std(close_price)












