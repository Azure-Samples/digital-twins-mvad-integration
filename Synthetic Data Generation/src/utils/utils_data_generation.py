"""utility functions to wrangle data or reformat df, or output files"""

import json
import os
from pathlib import Path
from datetime import timedelta
from datetime import datetime as dt
import pandas as pd
import numpy as np
from numpy.random import uniform, normal
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def generate_relationship_json(
    topo_df=None, save=True, output_data_path=None, output_json_file_name=None, output_csv_file_name=None
) -> None:
    """
    Generate relationship Json file based on topology table.

    Parameters
    ----------
    topo_df : topology table used to initialize the object,
        pd.DataFrame
    save : indicate whether to save the topology json file,
        bool, default=True
    output_data_path : path of output data folder,
        str
    output_json_file_name : name of the saved json file,
        str
    output_csv_file_name : name of the saved csv file,
        str

    Return
    ----------
    None
    """
    tmp_topo_df = topo_df.copy()
    tmp_topo_df["relationshipId"] = (
        tmp_topo_df["sourceId"]
        + "_"
        + tmp_topo_df["relationshipName"]
        + "_"
        + tmp_topo_df["targetId"]
    )
    tmp_topo_df = tmp_topo_df.rename(
        columns={
            "relationshipId": "$relationshipId",
            "sourceId": "$sourceId",
            "targetId": "$targetId",
            "relationshipName": "$relationshipName",
        }
    )
    tmp_topo_df["targetModel"] = "NA"
    tmp_topo_df["$etag"] = "NA"
    tmp_topo_df = tmp_topo_df[
        [
            "$relationshipId",
            "$sourceId",
            "$targetId",
            "$relationshipName",
            "targetModel",
            "$etag",
        ]
    ]
    ret_dic_lst = list(tmp_topo_df.to_dict("index").values())
    print(f"Sample Json File:\n{json.dumps(ret_dic_lst, indent=4)}")

    if save:
        Path(output_data_path).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(output_data_path, output_json_file_name), "w") as outfile:
            json.dump(ret_dic_lst, outfile, indent=4)
        topo_df.to_csv(output_data_path + output_csv_file_name, index=False)

def add_value_noise(x=None, accept_neg=False) -> float:
    """
    Helper function to add noise to simulated telemetry and potentially replace neg values by 0

    Parameters
    ----------
    x : original simulated telemetry value, float
    accept_neg, boolean: whether to accept negative values, if not, replace by 0

    Return
    ----------
    simulated telemetry with noise added, float
    """
    if x != 0:
        # Add noise as a random value from normal distribution for non-zero original value and make sure the output non-negative
        if accept_neg:  # KIP
            return x + normal(0, 0.1)
        else:
            return max(x + normal(0, 0.1), 0)
    else:
        # In a small probability add noise as a random value from normal distribution for zero original value, otherwise keep it as zero
        uniform_rv = uniform(0, 1)
        if uniform_rv <= 0.1:
            return normal(0, 0.05) if accept_neg else np.absolute(normal(0, 0.05))
        else:
            return 0


def add_timestamp_noise(df=None) -> pd.DataFrame:
    """
    Helper function to add timestamp noise to simulated dataframe

    Parameters
    ----------
    df : original dataframe with clean and standard column 'Timestamp',
        pd.DataFrame

    Return
    ----------
    df : dataframe with noisy timestamps,
        pd.DataFrame
    """
    # Add noise such as fractions of seconds to timestamp
    df["Microsecond"] = uniform(-(10**6), 10**6, df.shape[0])
    df["Timestamp"] = df.apply(
        lambda x: (x["Timestamp"] + timedelta(microseconds=x["Microsecond"])), axis=1
    )
    df = df.drop("Microsecond", axis=1)
    df = df.sort_values("Timestamp").reset_index(drop=True)
    return df


def random_drop_rows(df=None, missing_ratio=0) -> pd.DataFrame:
    """
    Helper function to randomly remove rows from simulated dataframe to create missings

    Parameters
    ----------
    df : original dataframe,
        pd.DataFrame
    missing_ratio : percentage of missing value of time-series,
        float, default=0

    Return
    ----------
    df : dataframe with certain rows randomly removed,
        pd.DataFrame
    """
    drop_indices = np.random.choice(
        df.index, int(df.shape[0] * missing_ratio), replace=False
    )
    df = df.drop(drop_indices)
    return df


def get_random_time_between(start_time=None, end_time=None) -> str:
    """
    Helper function to randomly pick a time within a time range regardless of date.

    Parameters
    ----------
    start_time : String of start time in the format of h:m:s,
        str (e.g. '07:00:00')
    end_time : String of start time in the format of h:m:s,
        str (e.g. '09:00:00')

    Return
    ----------
    Simulated time within the time range regardless of data in the format of h:m:s,
        str (e.g. '08:00:00')
    """
    h1, m1, s1 = start_time.split(":")
    h2, m2, s2 = end_time.split(":")
    total_seconds1 = int(h1) * 3600 + int(m1) * 60 + int(s1)
    total_seconds2 = int(h2) * 3600 + int(m2) * 60 + int(s2)
    start_of_day_total_seconds = int(uniform(total_seconds1, total_seconds2))
    start_of_day_h = start_of_day_total_seconds // 3600
    start_of_day_m = (start_of_day_total_seconds // 60) % 60
    start_of_day_s = (
        start_of_day_total_seconds - 3600 * start_of_day_h - 60 * start_of_day_m
    )
    start_of_day_h, start_of_day_m, start_of_day_s = (
        str(start_of_day_h),
        str(start_of_day_m),
        str(start_of_day_s),
    )
    return ":".join([start_of_day_h, start_of_day_m, start_of_day_s])


def plot_ts(ts_df=None, \
            anomaly_label=None, \
            start_time_str=None, \
            end_time_str=None, \
            id_lst=None, \
            key_lst=None, \
            plot_anomaly_label=True, \
            plot_results=False, \
            df_res=None, \
            floor_bin='15min', \
            severity_thres=None, \
            mode="lines", \
            height=800, \
            width=1000) -> None:
    """
    Make time-series plots for all nodes in graph as well as anomaly label and possibly with MVAD results.

    Parameters
    ----------
    ts_df : simulated time-series dataframe for all nodes in graph from function get_ts_df(),
        pd.DataFrame
    anomaly_label : one of simulated anomaly label time-series from function simulate_anomaly_labels(),
        pd.DataFrame
    start_time_str : start time to plot in string, if not given but plot_results=True then will take the start time of MVAD results,
        str (e.g. '2021-01-01 00:00:00')
    end_time_str : end time to plot in string, if not given but plot_results=True then will take the end time of MVAD results,
        str (e.g. '2021-02-01 00:00:00')
    id_lst : list of twin IDs included in plot,
        list of str (e.g. ['A', 'B', 'C'])
    key_lst : list of keys included in plot,
        list of str (e.g. ['Amps_Ia', 'Amps_Ib', 'Amps_Ic'])
    plot_anomaly_label : indicate whether to plot anomaly labels,
        bool, default=True
    plot_results : indicate whether to plot MVAD results,
        bool, default=False
    df_res : MVAD result dataframe, with columns: 'timestamp', 'errors', 'value.is_anomaly', 'value.severity', 'value.score', 'value.interpretation', 'result_id', 'summary',
        pd.DataFrame
    floor_bin : length of aggregation window for evaluation MVAD results,
        str, default='15min'
    severity_thres : specify the threshold of minimum severity to define an anomaly, if not given then use MVAD's default
        float (e.g. 0.1)
    mode: one of 'markers', 'lines' or 'lines_markers' for sensor time-series plots,
        str, default='lines'
    height, width : params that refer to Plotly

    Return
    ----------
    None
    """

    anomaly_label = anomaly_label.reset_index()
    anomaly_label['Timestamp'] = pd.to_datetime(anomaly_label['Timestamp'])
    ts_df['Timestamp'] = pd.to_datetime(ts_df['Timestamp'])

    if plot_results:
        df_res['timestamp'] = pd.to_datetime(df_res['timestamp']).dt.tz_localize(None)
        tmp_df_res = df_res[['timestamp', 'value.is_anomaly', 'value.severity']]
        tmp_df_res = tmp_df_res.sort_values('timestamp').reset_index(drop=True)
        severity_thres = severity_thres or tmp_df_res[tmp_df_res['value.is_anomaly']]['value.severity'].min()
        tmp_df_res.loc[tmp_df_res['value.severity']<severity_thres, 'value.is_anomaly'] = False
        
        label = anomaly_label.rename(columns={'Timestamp': 'timestamp'})
        tmp_label = label[(label['timestamp']>=tmp_df_res['timestamp'].iloc[0]) & \
                          (label['timestamp']<=tmp_df_res['timestamp'].iloc[-1])] \
                          .sort_values('timestamp').reset_index(drop=True)

        tmp_df_res['timestamp_floor'] = tmp_df_res['timestamp'].dt.floor(floor_bin)
        df_res_eval = pd.DataFrame(tmp_df_res.groupby('timestamp_floor')['value.is_anomaly'].apply(lambda x: any(list(x))))
        tmp_label['timestamp_floor'] = tmp_label['timestamp'].dt.floor(floor_bin)
        label_eval = pd.DataFrame(tmp_label.groupby('timestamp_floor')['isAnomaly'].apply(lambda x: 1 in list(x)))

        assert df_res_eval.index.equals(label_eval.index), "Caution! Timestamps don't match."
        df_res_label_eval = pd.concat([df_res_eval, label_eval], axis=1)
        df_res_label_eval['confusion'] = df_res_label_eval.apply(lambda x: 'TP' if x['value.is_anomaly']==x['isAnomaly']==True
                                                                                else ('FP' if (x['value.is_anomaly']== True) and (x['isAnomaly']==False)
                                                                                           else ('TN' if x['value.is_anomaly']==x['isAnomaly']==False else 'FN')), axis=1)
        start_time, end_time = df_res_label_eval.index[0], df_res_label_eval.index[-1]

    if start_time_str is None:
        if plot_results:
            anomaly_label = anomaly_label[anomaly_label['Timestamp']>=start_time]
            ts_df = ts_df[ts_df['Timestamp']>=start_time]
    else:
        start_time = dt.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
        anomaly_label = anomaly_label[anomaly_label['Timestamp']>=start_time]
        ts_df = ts_df[ts_df['Timestamp']>=start_time]
        if plot_results:
            df_res_label_eval = df_res_label_eval[df_res_label_eval.index>=start_time]

    if end_time_str is None:
        if plot_results:
            anomaly_label = anomaly_label[anomaly_label['Timestamp']<=end_time]
            ts_df = ts_df[ts_df['Timestamp']<=end_time]
    else:
        end_time = dt.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
        anomaly_label = anomaly_label[anomaly_label['Timestamp']<=end_time]
        ts_df = ts_df[ts_df['Timestamp']<=end_time]
        if plot_results:
            df_res_label_eval = df_res_label_eval[df_res_label_eval.index<=end_time]

    anomaly_label = anomaly_label.set_index('Timestamp')

    if id_lst is not None:
        ts_df = ts_df[ts_df['Id'].isin(id_lst)]
    if key_lst is not None:
        ts_df = ts_df[ts_df['Key'].isin(key_lst)]
    ts_df_gb = ts_df.groupby(["Id", "Key"])
    sorted_gb_keys = sorted(ts_df_gb.groups.keys())

    title_text = 'Inference Visualization with MVAD Results' if plot_results else 'Simulated Time-series Sensor Telemetry with Anomalies'
    if plot_anomaly_label:
        fig = make_subplots(
            rows=2, cols=1, 
            subplot_titles=["Simulated Anomaly Labels", 'Simulated Time-series Sensor Telemetry'], 
            row_heights=[1,8], vertical_spacing=0.1
            )
        fig.add_trace(
            go.Scatter(
                x=anomaly_label.index,
                y=anomaly_label.isAnomaly,
                name="ANOMALY LABEL",
                mode="markers",
            ),
            row=1,
            col=1,
        )
    else:
        fig = make_subplots(
            rows=1, cols=1
            )

    num_subplots = 2 if plot_anomaly_label else 1
    for gb_key in sorted_gb_keys:
        sub_ts_df = ts_df_gb.get_group(gb_key).sort_values("Timestamp")
        try:
            sub_ts_df["Value"] = sub_ts_df["Value"].astype('float')
        except:
            pass
        fig.add_trace(
            go.Scatter(
                x=sub_ts_df["Timestamp"],
                y=sub_ts_df["Value"],
                name="_".join(gb_key),
                mode=mode,
                ),
            row=num_subplots,
            col=1
        )

    if plot_results:
        for j in range(df_res_label_eval.shape[0]-1):
            confusion = df_res_label_eval.iloc[j]['confusion']
            if confusion == 'TN':
                vrect_color = 'green'
            elif confusion == 'FN':
                vrect_color = 'yellow'
            elif confusion == 'FP':
                vrect_color = 'grey'
            else:
                vrect_color = 'red'

            fig.add_vrect(
                x0=df_res_label_eval.index[j],
                x1=df_res_label_eval.index[j+1],
                fillcolor=vrect_color, 
                opacity=0.5,
                layer="below", 
                line_width=0,
                row=num_subplots, 
                col=1
            )

    fig.update_layout(
        height=height, width=width, title_text=title_text, title_x=0.5
    )
    fig.show()
