# -*- coding: utf-8 -*-

"""
Base methods and classes to download data from different APIs

"""
from src.code.utils.utils import get_yaml_value, write_data, read_data
import platform
import os
import pandas as pd


class BaseDownloader:

    """
    Contains common methods to dowload data from different APIs
    """

    def __init__(self, format_: str,
                 interval: str):

        self.file_path = None
        self.file_name = None
        self.platform = platform.system()
        self.format = format_
        self.interval = interval

        self.opj = os.path.join

    def get_file_path(self, tic, downloader_name, identifier):

        """Generate output path for the data to be written
        """
        self.file_name = '{}_{}_{}_{}.{}'.format("raw", downloader_name, self.interval, identifier,self.format)
        folder_path = self.opj(get_yaml_value("path", "data"), get_yaml_value("path", "input"), tic, downloader_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        file_path = self.opj(folder_path, self.file_name)

        return file_path            

    def rename_columns(self):

        """Make the column names uniform - 
        """
        return

    def write_data(self, dataframe, tic, downloader_name, identifier):

        """Write the data to the folder/ file
        """
        
        os_path_exists = os.path.exists(self.get_file_path(tic, downloader_name, identifier))
        self.file_path = self.get_file_path(tic, downloader_name, identifier)
        if not os_path_exists:
            write_data(dataframe, self.file_path, self.format)
        else:

            dataframe_existing = read_data(dataframe, self.file_path, self.format)
            dataframe_append = pd.concat(
                [dataframe_existing, dataframe], axis=0, ignore_index = True)
                
            dataframe_append.drop_duplicates(inplace=True)
            write_data(dataframe_append, self.file_path, self.format)
