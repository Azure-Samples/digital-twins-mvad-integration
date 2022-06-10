#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import datetime as dt
from numpy.random import uniform
from utils.utils_data_generation import add_timestamp_noise, random_drop_rows, get_random_time_between

def get_binary_ts_df(num_simulated_ts=1, \
                     time_range_lst=None, \
                     freq_lst=None, \
                     id_name_lst=None, \
                     key_name_lst=None, \
                     start_of_day_range_lst=None, \
                     end_of_day_range_lst=None, \
                     on_occurrence_range_lst=None, \
                     on_length_range_lst=None, \
                     missing_ratio_lst=[0], \
                     timestamp_noise_lst=[False]) -> pd.DataFrame:
    """
    Get simulated binary time-series dataframe for selected sensors.

    Parameters
    ----------
    num_simulated_ts : number of binary time-series to simulate, 
        int, default=1
    time_range_lst : list of time-range for each binary series, 
        list of list of str (e.g. [['2020-07-01 00:00:00', '2021-07-01 00:00:00'] for _ in range(2)])
    freq_lst : list of telemetry updating frequency (sample rate) for each binary series, 
        list of str (e.g. ['10min', '10min'])
    id_name_lst : name of Ids (e.g. device name),
        list of str (e.g. ['A', 'B'])
    key_name_lst : name of keys (e.g. channel of sensor),
        list of str (e.g. ['Status', 'Status'])
    start_of_day_range_lst : list of the range of weekday start time for each anomaly label series, 
        list of list of str (e.g. [['07:00:00', '09:00:00'] for _ in range(2)])
    end_of_day_range_lst : list of the range of weekday end time for each anomaly label series, 
        list of list of str (e.g. [['16:00:00', '17:00:00'] for _ in range(2)])
    on_occurrence_range_lst : list of 'ON' occurrences range,
        list of list of int (e.g. [[5, 7] for _ in range(2)])
    on_length_range_lst : list of range of duration keeping 'ON', in terms of number of consecutive timestamps for each binary series,
        list of list of int (e.g. [[5, 10] for _ in range(2)])
    missing_ratio_lst : list of percentage of missing value of time-series, 
        list of float, default=[0]
    timestamp_noise_lst : list of indicator of whether to add noise (e.g. fractions of seconds) into timestamp,
        list of bool, default=[False]

    Return
    ----------
    ret_binary_ts_df : simulated binary time-series dataframe for selected sensors with columns=['Timestamp', 'Id', 'Value', 'Key'], 
        pd.DataFrame
    """
    ret_binary_ts_df = pd.DataFrame()
    for sensor in range(num_simulated_ts):
        time_range, freq, id_name, key_name, start_of_day_range, end_of_day_range, on_occurrence_range, on_length_range, missing_ratio, timestamp_noise = time_range_lst[sensor], \
                                                                                                                                                          freq_lst[sensor], \
                                                                                                                                                          id_name_lst[sensor], \
                                                                                                                                                          key_name_lst[sensor], \
                                                                                                                                                          start_of_day_range_lst[sensor], \
                                                                                                                                                          end_of_day_range_lst[sensor], \
                                                                                                                                                          on_occurrence_range_lst[sensor], \
                                                                                                                                                          on_length_range_lst[sensor], \
                                                                                                                                                          missing_ratio_lst[sensor], \
                                                                                                                                                          timestamp_noise_lst[sensor]
        print(f'Simulating Binary Labels for Sensor {sensor}...')
        binary_ts_df = pd.DataFrame(columns=['Timestamp'], \
                                    data=pd.date_range(time_range[0], time_range[1], freq=freq))
        binary_ts_df['date'] = binary_ts_df['Timestamp'].dt.date
        binary_ts_df['Id'] = id_name
        binary_ts_df['Key'] = key_name
        binary_ts_df['Value'] = 0

        # Simulate start and end time of weekday
        start_of_day_str, end_of_day_str = get_random_time_between(start_of_day_range[0], start_of_day_range[1]), \
                                           get_random_time_between(end_of_day_range[0], end_of_day_range[1])
        start_of_day, end_of_day = dt.strptime(start_of_day_str, '%H:%M:%S').time(),\
                                   dt.strptime(end_of_day_str, '%H:%M:%S').time()
        # print(f'For Binary TS: Simulated Start & End of Weekday: {start_of_day}, {end_of_day}')
        # Differentiate on-hour and off-hour
        on_hours = binary_ts_df[~(binary_ts_df['Timestamp'].dt.weekday.isin([5,6])) \
                                &(binary_ts_df['Timestamp'].dt.time>=start_of_day) \
                                &(binary_ts_df['Timestamp'].dt.time<=end_of_day)].reset_index(drop=True)
        off_hours = binary_ts_df[(binary_ts_df['Timestamp'].dt.weekday.isin([5,6])) \
                                 |(binary_ts_df['Timestamp'].dt.time<start_of_day)\
                                 |(binary_ts_df['Timestamp'].dt.time>end_of_day)].set_index('Timestamp')
        on_hours['isOffHour'], off_hours['isOffHour'] = False, True
        # Simulate the number of being on
        on_occurrence = int(uniform(on_occurrence_range[0], on_occurrence_range[1]+1, 1))
        # Simulate the numerical indices when device is turned on
        on_start_indices = np.sort(np.random.choice(on_hours.shape[0], on_occurrence))
        # Simulate the duration of each 'ON' status
        on_lengths = uniform(on_length_range[0], on_length_range[1], on_occurrence)
        # Simulate the binary ts
        for j in range(on_occurrence):
            on_hours.loc[on_start_indices[j]:on_start_indices[j]+on_lengths[j]-1, 'Value'] = 1
        on_hours = on_hours.set_index('Timestamp')
        # Combine on-hour and off-hour for anomaly label and sort by timestamp
        binary_ts_df = pd.concat([on_hours, off_hours]).sort_index().reset_index()
        
        # Randomly remove rows to simulate missings
        binary_ts_df = random_drop_rows(binary_ts_df, missing_ratio)
        # Add noise such as fractions of seconds to timestamp
        if timestamp_noise:
            binary_ts_df = add_timestamp_noise(binary_ts_df)
        print(f'Sensor {sensor} Binary Simulation Done.')
        ret_binary_ts_df = pd.concat([ret_binary_ts_df, binary_ts_df])
    ret_binary_ts_df = ret_binary_ts_df.sort_values('Timestamp').reset_index(drop=True)
    ret_binary_ts_df = ret_binary_ts_df[['Id', 'Key', 'Timestamp', 'Value']]

    return ret_binary_ts_df
