#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
from datetime import datetime as dt
from numpy.random import uniform
from utils.utils_data_generation import get_random_time_between

def simulate_anomaly_labels(num_simulated_anomaly_ts=None, \
                            time_range_lst=None, \
                            freq_lst=None, \
                            start_of_day_range_lst=None, \
                            end_of_day_range_lst=None, \
                            surge_occurrence_range_lst=None, \
                            surge_length_range_lst=None) -> tuple:
    """
    Simulate binary anomaly label series.

    Parameters
    ----------
    num_simulated_anomaly_ts : number of binary time-series anomaly label series to simulate, 
        int (e.g. 1)
    time_range_lst : list of time-range for each anomaly label series, 
        list of list (e.g. [['2020-07-01 00:00:00', '2021-07-01 00:00:00'] for _ in range(2)])
    freq_lst : list of telemetry updating frequency (sample rate) for each anomaly label series, 
        list of str (e.g. ['10min' for _ in range(2)])
    start_of_day_range_lst : list of the range of weekday start time for each anomaly label series, 
        list of list of str (e.g. [['07:00:00', '09:00:00'] for _ in range(2)])
    end_of_day_range_lst : list of the range of weekday end time for each anomaly label series, 
        list of list of str (e.g. [['16:00:00', '17:00:00'] for _ in range(2)])
    surge_occurrence_range_lst : list of surge occurrences range,
        list of list of int (e.g. [[5, 7] for _ in range(2)])
    surge_length_range_lst : list of range of surge duration, in terms of number of consecutive timestamps for each anomaly label series,
        list of list of int (e.g. [[5, 10] for _ in range(2)])

    Return
    ----------
    Tuple of (anomaly_label_lst, surge_start_end_indices_lst)
    anomaly_label_lst : list of anomaly label series simulated, each as dataframe with columns=['date', 'isAnomaly', 'isOffHour'] and Timestamp as index,
        list of DataFrame
    surge_start_end_indices_lst : list of start and end timestamps for each surge occurrence for each anomaly label series,
        list of list of of list of Timestamps (e.g. [
                                                        [
                                                        [Timestamp('2020-07-20 15:50:00'), Timestamp('2020-07-21 09:20:00')],
                                                        [Timestamp('2020-07-21 13:10:00'), Timestamp('2020-07-21 13:50:00')],
                                                        [Timestamp('2020-08-26 09:20:00'), Timestamp('2020-08-26 10:20:00')]
                                                        ]
                                                    ])
    """
    anomaly_label_lst, surge_start_end_indices_lst = [], []
    for ts_i in range(num_simulated_anomaly_ts):
        time_range, freq, start_of_day_range, end_of_day_range, surge_occurrence_range, surge_length_range = time_range_lst[ts_i], \
                                                                                                             freq_lst[ts_i], \
                                                                                                             start_of_day_range_lst[ts_i], \
                                                                                                             end_of_day_range_lst[ts_i], \
                                                                                                             surge_occurrence_range_lst[ts_i], \
                                                                                                             surge_length_range_lst[ts_i]
        print(f'Simulating Anomaly Labels for #{ts_i}...')
        anomaly_label = pd.DataFrame(columns=['Timestamp'], \
                                    data=pd.date_range(time_range[0], time_range[1], freq=freq))
        anomaly_label['date'] = anomaly_label['Timestamp'].dt.date
        anomaly_label['isAnomaly'] = 0

        # Simulate start and end time of weekday
        start_of_day_str, end_of_day_str = get_random_time_between(start_of_day_range[0], start_of_day_range[1]), \
                                           get_random_time_between(end_of_day_range[0], end_of_day_range[1])
        start_of_day, end_of_day = dt.strptime(start_of_day_str, '%H:%M:%S').time(),\
                                   dt.strptime(end_of_day_str, '%H:%M:%S').time()
        # print(f'For Labels: Simulated Start & End of Weekday: {start_of_day}, {end_of_day}')
        # Differentiate on-hour and off-hour
        on_hours = anomaly_label[~(anomaly_label['Timestamp'].dt.weekday.isin([5,6])) \
                                 &(anomaly_label['Timestamp'].dt.time>=start_of_day) \
                                 &(anomaly_label['Timestamp'].dt.time<=end_of_day)].reset_index(drop=True)
        off_hours = anomaly_label[(anomaly_label['Timestamp'].dt.weekday.isin([5,6])) \
                                  |(anomaly_label['Timestamp'].dt.time<start_of_day)\
                                  |(anomaly_label['Timestamp'].dt.time>end_of_day)].set_index('Timestamp')
        on_hours['isOffHour'], off_hours['isOffHour'] = False, True
        # Simulate the number of surges
        surge_occurrence = int(uniform(surge_occurrence_range[0], surge_occurrence_range[1]+1, 1))
        # Simulate the numerical indices when surges start
        surge_start_indices = np.sort(np.random.choice(on_hours.shape[0], surge_occurrence))
        # Simulate the duration of each surge
        surge_lengths = uniform(surge_length_range[0], surge_length_range[1], surge_occurrence)
        # Get start and end timestamps for each surge occurrence
        surge_start_end_indices = []
        for j in range(surge_occurrence):
            on_hours.loc[surge_start_indices[j]:surge_start_indices[j]+surge_lengths[j]-1, 'isAnomaly'] = 1
            tmp_on_hours = on_hours.loc[surge_start_indices[j]:surge_start_indices[j]+surge_lengths[j]-1]
            surge_start_end_indices.append([tmp_on_hours['Timestamp'].iloc[0], tmp_on_hours['Timestamp'].iloc[-1]])
        on_hours = on_hours.set_index('Timestamp')
        # Combine on-hour and off-hour for anomaly label and sort by timestamp
        anomaly_label = pd.concat([on_hours, off_hours]).sort_index()
        for k in range(surge_occurrence):
            anomaly_label.loc[surge_start_end_indices[k][0]: surge_start_end_indices[k][1], 'isAnomaly'] = 1
        anomaly_label_lst.append(anomaly_label)
        surge_start_end_indices_lst.append(surge_start_end_indices)
        print(f'#{ts_i} Anomaly Labels Simulation Done.')

    return anomaly_label_lst, surge_start_end_indices_lst
