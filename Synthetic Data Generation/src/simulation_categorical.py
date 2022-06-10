#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from utils.utils_data_generation import random_drop_rows, add_timestamp_noise

def get_cat_ts_df(anomaly_label_lst=None, \
                  num_simulated_ts=1, \
                  freq_lst=None, \
                  id_name_lst=None, \
                  key_name_lst=None, \
                  cat_names_lst=None, \
                  cat_ratio_lst=None, \
                  missing_ratio_lst=[0], \
                  timestamp_noise_lst=[False]) -> pd.DataFrame:
    """
    Get simulated categorical time-series dataframe for selected sensors.

    Parameters
    ----------
    anomaly_label_lst : list of anomaly label series simulated from function simulate_anomaly_labels(),
        list of DataFrame
    num_simulated_ts : number of categorical time-series to simulate, 
        int, default=1
    freq_lst : list of telemetry updating frequency (sample rate),
        list of str (e.g. ['10min', '10min'])
    id_name_lst : name of Ids (e.g. device name),
        list of str (e.g. ['A', 'B'])
    key_name_lst : name of keys (e.g. channel of sensor),
        list of str (e.g. ['PowerLevel', 'PowerLevel'])
    cat_names_lst : list of list of all possible values of categorical,
        list of list of str (e.g. [['High', 'Mid', 'Low'], ['High', 'Mid', 'Low']])
    cat_ratio_lst : list of list of ratios of each category,
        list of list of float (e.g. [[1/3, 1/3, 1/3], [1/3, 1/3, 1/3]])
    missing_ratio_lst : list of percentage of missing value of time-series, 
        list of float, default=[0]
    timestamp_noise_lst : list of indicator of whether to add noise (e.g. fractions of seconds) into timestamp,
        list of bool, default=[False]

    Return
    ----------
    ret_cat_ts_df : simulated categorical time-series dataframe for selected sensors with columns=['Timestamp', 'Id', 'Value', 'Key'], 
        pd.DataFrame
    """
    ret_cat_ts_df = pd.DataFrame()
    for sensor in range(num_simulated_ts):
        freq, id_name, key_name, cat_names, cat_ratio, missing_ratio, timestamp_noise = freq_lst[sensor], \
                                                                                        id_name_lst[sensor], \
                                                                                        key_name_lst[sensor], \
                                                                                        cat_names_lst[sensor], \
                                                                                        cat_ratio_lst[sensor], \
                                                                                        missing_ratio_lst[sensor], \
                                                                                        timestamp_noise_lst[sensor]
        cat_ts_df = pd.DataFrame(pd.date_range(anomaly_label_lst[0].index[0], \
                                               anomaly_label_lst[0].index[-1], \
                                               freq=freq), columns=['Timestamp'])
        cat_ts_df['Id'] = id_name
        cat_ts_df['Key'] = key_name
        tmp_value_lst = []
        for i, cat in enumerate(cat_names):
            if i!= len(cat_names)-1:
                tmp_value_lst = tmp_value_lst + [cat_names[i] for _ in range(int(cat_ts_df.shape[0]*cat_ratio[i]))]
        tmp_value_lst = tmp_value_lst + [cat_names[i] for _ in range(cat_ts_df.shape[0]-len(tmp_value_lst))]
        np.random.shuffle(tmp_value_lst)
        cat_ts_df['Value'] = tmp_value_lst
        
        # Randomly remove rows to simulate missings
        cat_ts_df = random_drop_rows(cat_ts_df, missing_ratio)
        # Add noise such as fractions of seconds to timestamp
        if timestamp_noise:
            cat_ts_df = add_timestamp_noise(cat_ts_df)
        print(f'Sensor {sensor} Categorical Simulation Done.')
        ret_cat_ts_df = pd.concat([ret_cat_ts_df, cat_ts_df])
    ret_cat_ts_df = ret_cat_ts_df.sort_values('Timestamp').reset_index(drop=True)
    ret_cat_ts_df = ret_cat_ts_df[['Id', 'Key', 'Timestamp', 'Value']]

    return ret_cat_ts_df
