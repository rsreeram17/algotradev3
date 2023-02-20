# -*- coding: utf-8 -*-

"""
Base methods and classes to build features for OHLCV data

"""
from src.code.utils.utils import get_yaml_value, write_data, read_data
import os
import pandas as pd
import numpy as np


class BaseFeatureGenerator:

    def __init__(self, format_: str,
                 interval: str,
                 close_column_name: str = "close"):

        self.format = format_
        self.interval = interval
        self.close_column_name = close_column_name
        self.opj = os.path.join

        self.latest_features_fpath = self.opj(
            get_yaml_value("path", "features"), "latest_features.{}".format(self.format))

        self.historical_features_fpath = self.opj(
            get_yaml_value("path", "features"), "historical_features.{}".format(self.format))

        self.latest_features_data = read_data(self.latest_features_fpath, self.format)
        self.latest_price_data = self.latest_features_data

    def calculate_sma(self, ticker: str, latest_price_data: pd.DataFrame, ma_type: int = 20):

        """
        Calculates simple moving average for the latest timestep

        https://www.investopedia.com/terms/s/sma.asp

        :param ticker: Company ticker
        :param latest_price_data: Price data (OHLCV) including the latest time step sorted by the date column
        :param ma_type: Moving average type - For eg. 20,50, 200 etc
        :return: moving average for the latest timestep
        """

        ticker_latest_info = latest_price_data[latest_price_data["ticker"] == ticker]
        close_price = np.array(ticker_latest_info.head(ma_type)[self.close_column_name])
        return np.mean(close_price)




