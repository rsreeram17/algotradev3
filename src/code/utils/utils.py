import os
from src.code.utils.sys_utils import read_config
import pandas as pd
import json
import pickle
from datetime import datetime


def get_yaml_value(filename: str, key: str):

    cfg = read_config(filename)
    value = cfg.get(key)
    return value


def create_date_chunks(start_date: str,
                       end_date: str,
                       chunk_length: int):

    """

    :param start_date:
    :param end_date:
    :param chunk_length:
    :return:
    """



    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    date_list = list(pd.date_range(start_date, end_date, freq='d'))
    date_list = [x.strftime('%Y-%m-%d') for x in date_list]

    chunks = [date_list[x:x+chunk_length] for x in range(0, len(date_list), chunk_length)]

    return chunks


def create_lagged_features(df, columns, trailing_window = 1):

    data_lagged = df.copy()
    for window in range(1, trailing_window + 1):
        shifted = df[columns].shift(window)
        shifted.columns = [x + "_lag" + str(window) for x in columns]
        data_lagged = pd.concat((data_lagged, shifted), axis=1)

    return data_lagged

def normalize_columns(df, columns):

    for column in columns:
        column_names = [col for col in df.columns if column in col]
        data_frame_subset = df[column_names]
        df[column] = df[column]/data_frame_subset.mean(axis=0)

#def cache_info(json_fname, key, value):
#
#    opj = os.path.join
#    file_path = opj(extract_folder_path("cache"), json_fname)
#
#    with open(file_path, "r") as json_file:
#        data = json.load(json_file)
#
#        if isinstance(key, list):
#            for i, key_ in enumerate(key):
#                data[key_] = value[i]
#        else:
#            data[key] = value
#
#    with open(file_path, "w") as json_file:
#        json.dump(data, json_file)

def dump_pickle(path,obj):

    file = open(path, "wb")
    pickle.dump(obj, file)
    file.close()

def read_pickle(path):

    file = open(path,"rb")
    data = pickle.load(file)
    return data


def write_data(dataframe: pd.DataFrame, 
               filename,
               format:str):
    
    if format == "ftr":
        dataframe.to_feather(filename)
    elif format == "csv":
        dataframe.to_csv(filename)


def read_data(filename,
              format: str):
    if format == "ftr":
        data = pd.read_feather(filename)
    elif format == "csv":
        data = pd.read_csv(filename)
    else:
        data = None
    return data




