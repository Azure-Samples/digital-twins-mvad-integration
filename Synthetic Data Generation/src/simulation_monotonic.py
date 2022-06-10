#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from simulation_continuous import get_cont_ts_df
from utils.utils_data_generation import random_drop_rows

def get_monotonic_ts_df(unique_anomaly_label=True, \
                        anomaly_label_lst=None, \
                        surge_start_end_indices_lst=None, \
                        simulate_surge_lst=None, \
                        surge_ratio_range_lst=None, \
                        normal_mean_range_lst=None, \
                        normal_std_lst=None, \
                        gd=None, \
                        key_name=None, \
                        missing_ratio=0, \
                        value_noise=True,\
                        timestamp_noise=False) -> pd.DataFrame:
    """
    Simulate monotonic time-series based on anomaly labels and continuous simulation.

    Parameters
    ----------
    unique_anomaly_label : indicate whether all time-series to be simulated according to the same anomaly label series,
        if set as False then the number of simulated sensors and the number of simulated anomaly label series must match.
        bool, default=True
    anomaly_label_lst : list of anomaly label series simulated from function simulate_anomaly_labels(),
        list of DataFrame
    surge_start_end_indices_lst : list of start and end timestamps for each surge occurrence for each anomaly label series from function simulate_anomaly_labels(),
        list of list of of list of Timestamps
    simulate_surge_lst : indicate whether to simulate surge for each simulated time-series,
        list of bool (e.g. [True, True])
    surge_ratio_range_lst : list of the range of surge degree for each simulated time-series,
        list of list of float (e.g. [[10.0, 20.0], [20.0, 30.0]])
    normal_mean_range_lst : list of the mean value during normal times for each simulated time-series,
        list of list of float (e.g. [[6.0, 7.0], [26.0, 27.0]])
    normal_std_lst : list of the standard deviation during normal times for each simulated time-series,
        list of list of float (e.g. [0.5, 0.5])
    gd : an instance of GraphDataset object,
        GraphDataset
    key_name : name of key (e.g. channel of sensor)
        str, (e.g. 'PowerMeter')
    missing_ratio : percentage of missing value of time-series, 
        float, default=0
    value_noise : indicate whether to add noise to the simulated telemetry,
        bool, default=True
    timestamp_noise : indicate whether to add noise (e.g. fractions of seconds) into timestamp,
        bool, default=False

    Return
    ----------
    ret_monotonic_ts_df : simulated monotonic time-series dataframe for selected sensors with columns=['Timestamp', 'Id', 'Value', 'Key'], 
        pd.DataFrame
    """
    ts_df = get_cont_ts_df(unique_anomaly_label=unique_anomaly_label, \
                           anomaly_label_lst=anomaly_label_lst, \
                           surge_start_end_indices_lst=surge_start_end_indices_lst, \
                           simulate_surge_lst=simulate_surge_lst, \
                           surge_ratio_range_lst=surge_ratio_range_lst, \
                           normal_mean_range_lst=normal_mean_range_lst, \
                           normal_std_lst=normal_std_lst, \
                           gd=gd, \
                           key_name=key_name, \
                           missing_ratio=0, \
                           value_noise=value_noise,\
                           timestamp_noise=timestamp_noise)

    ts_df_gb = ts_df.groupby('Id')
    ret_monotonic_ts_df = pd.DataFrame()
    for gb_key in ts_df_gb.groups.keys():
        sub_ts_df_monotonic = ts_df_gb.get_group(gb_key).sort_values('Timestamp').reset_index(drop=True)
        sub_ts_df_monotonic['Value'] = sub_ts_df_monotonic['Value'].cumsum()
        ret_monotonic_ts_df = pd.concat([ret_monotonic_ts_df, sub_ts_df_monotonic])
    # update_stream_monotonic['Key'] = 'PowerMeter'
    ret_monotonic_ts_df = random_drop_rows(ret_monotonic_ts_df, missing_ratio)
    ret_monotonic_ts_df = ret_monotonic_ts_df[['Id', 'Key', 'Timestamp', 'Value']]
    ret_monotonic_ts_df = ret_monotonic_ts_df.sort_values('Timestamp').reset_index(drop=True)
    
    return ret_monotonic_ts_df
