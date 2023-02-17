import pandas as pd
from src.code.data.download.base import BaseDownloader
import os
from src.code.data.download.fmp_python.fmp import *
from src.code.utils.utils import create_date_chunks
from tqdm import tqdm
from multiprocessing import Pool
import time

class FMPDownloader(BaseDownloader):

    def __init__(self,
                 format_: str,
                 interval: str, 
                 api_info: str):
        BaseDownloader.__init__(self, format_=format_, interval=interval)

        self.end_date = None
        self.start_date = None
        self.ticker_list = None
        self.identifier = None
        self.tic = None
        self.fmp_obj = FMP(api_info)
        self.downloader_name = "FMP"

    def download_historical_price(self, ticker_list, start_date, end_date):

        self.start_date = start_date
        self.end_date = end_date
        self.ticker_list = ticker_list
        parameters = ""
        self.identifier = "bulk"

        if self.start_date is not None and self.end_date is not None:
            self.identifier = "{}_{}".format(self.start_date, self.end_date)
            parameters = {"from": self.start_date,
                          "to": self.end_date}
        
        for self.tic in tqdm(self.ticker_list):
            if self.interval == "1d":
                data = self.fmp_obj.get_historical_price(symbol=self.tic, param=parameters)
            else:
                data = self.fmp_obj.get_historical_chart(interval=self.interval, symbol=self.tic, param=parameters)

            self.write_data(dataframe=data,
                            tic=self.tic,
                            downloader_name=self.downloader_name,
                            identifier=self.identifier)


if __name__ == "__main__":

    fmp_downloader = FMPDownloader(format_="csv",
                                   interval="5min",
                                   api_info="610077b8dbc34769b5f44606522e4920")

    start_date = "2005-01-01"
    end_date = "2005-12-31"

    dates = [("{}-01-01".format(year), "{}-12-31".format(year)) for year in range(2012, 2022)]
    #dates = [("2010-01-01", "2010-12-31")]

    ticker_list = ["TSLA", "AMD", "SBUX"]

    for start_date, end_date in dates:
        date_chunks = create_date_chunks(start_date, end_date, 8)
        with Pool() as pool:
            items = [(ticker_list, y[0], y[-1]) for y in date_chunks]
            pool.starmap(fmp_downloader.download_historical_price, items)
        time.sleep(70)


