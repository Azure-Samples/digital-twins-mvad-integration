#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import datetime as dt

def get_pattern_anomalies(df=None, \
                          Id=None, \
                          Key=None, \
                          timerange_str_lst=None, \
                          anomalies_type_lst=None, \
                          magnitude_lst=None) -> pd.DataFrame:
    
    """
    Add pattern anomalies to the simulated time-series in two ways, either 'assign' new values directly or modify the old values by multiplication.
    Either way it will break the topology relationships within a subset of graph for a period and thereby generate a pattern anomaly instead of single-point wise surge or drop.

    Parameters
    ----------
    df : dataframe before pattern anomalies addition,
        pd.DataFrame
    Id : twin id to apply pattern anomalies,
        str
    Key : channel key to apply pattern anomalies,
        str
    timerange_str_lst : the start and end timestamp in string for each pattern anomaly period,
        list of list of str (e.g. [['2021-01-01 00:00:00', '2021-01-01 00:10:00'], ['2021-01-31 23:55:00', '2021-01-31 23:58:00']])
    anomalies_type_lst : for each pattern anomalies indicate whether to assign a value for that period - 'assign', or update the original values by a multiple - 'multiple',
        list of str (e.g. ['assign', 'multiple'])
    magnitude_lst : specify the assigned value or multiple for each pattern anomaly period,
        list of float (e.g. [10, 1/2])

    Return
    ----------
    ret_df : dataframe after pattern anomalies added, 
        pd.DataFrame
    """
    assert len(timerange_str_lst)==len(anomalies_type_lst)==len(magnitude_lst)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    timerange_lst = [[dt.strptime(timerange_str[0], '%Y-%m-%d %H:%M:%S'), dt.strptime(timerange_str[1], '%Y-%m-%d %H:%M:%S')] for timerange_str in timerange_str_lst]
    df_changed = df[(df['Id']==Id) & (df['Key']==Key)].sort_values('Timestamp').reset_index(drop=True)
    df_unchanged = df[(df['Id']!=Id) | (df['Key']!=Key)]
    
    for i, timerange in enumerate(timerange_lst):
        timerange_indices = df_changed[(df_changed['Timestamp']>=timerange[0]) & \
                                       (df_changed['Timestamp']<=timerange[1])].index
        if anomalies_type_lst[i]=='assign':
            df_changed.loc[timerange_indices, 'Value'] = magnitude_lst[i]
        else:
            df_changed.loc[timerange_indices, 'Value'] = magnitude_lst[i] * df_changed.loc[timerange_indices, 'Value']
    ret_df = pd.concat([df_changed, df_unchanged]).sort_values('Timestamp').reset_index(drop=True)
    
    return ret_df
    