"""
Functions to generate continuous time-series data profile, with anomalies
"""
import networkx as nx
from numpy.random import uniform, normal
import numpy as np
import pandas as pd

from utils.utils_data_generation import (
    add_value_noise,
    add_timestamp_noise,
    random_drop_rows,
)
from utils.gen_ts_shapes import (
    gen_beta_anom,
    get_wave_period,
    gen_cosine_imperfect,
    gen_pw_concave_trend,
    gen_cosine_trend,
)

pd.options.mode.chained_assignment = None


def simulate_source_nodes_ts(
    unique_anomaly_label=True,
    anomaly_label_lst=None,
    surge_start_end_indices_lst=None,
    simulate_surge_lst=None,
    surge_ratio_range_lst=None,
    normal_mean_range_lst=None,
    normal_std_lst=None,
    gd=None,
    ts_shape="sinusoidal",
    add_season_trend=False,
    sine_oh_params=None,
    freq_lst=None,
    surge_with_decay=False,
) -> list:
    """
    Simulate telemetry time-series for selected source nodes based on anomaly labels.

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
    ts_shape, str: time-series shapes to mimic, currently options are "sinusoidal", "steps",
    add_season_trend, boolean: whether to add the yearly sine pattern
    sine_oh_params, dict: dictionary of params for creating daily sine pattern, yearly sine pattern (if any), and concave week off-hours, and weekend hours
    freq_lst, str: list of the sensor sampling rate/frequency for each simulated time-series (sensor)
    surge_with_decay, boolean: whether to turn on the inclusion of surge/dip of decaying anomalous behavior as anomaly


    Return
    ----------
    ret_df_lst : list of simulated time-series dataframes, each with columns=['date', 'isAnomaly', 'isOffHour', 'value'] and Timestamp as index,
        list of Dataframe
    """
    ret_df_lst = []
    num_simulated_sensors = len(gd.simulated_nodes)
    for sensor in range(num_simulated_sensors):
        print(f"\nSimulating Time-series for Sensor {sensor}...")
        if unique_anomaly_label:
            anomaly_label, surge_start_end_indices = (
                anomaly_label_lst[0],
                surge_start_end_indices_lst[0],
            )
        else:
            anomaly_label, surge_start_end_indices = (
                anomaly_label_lst[sensor],
                surge_start_end_indices_lst[sensor],
            )
        if simulate_surge_lst is not None:
            simulate_surge, surge_ratio_range, normal_mean_range, normal_std = (
                simulate_surge_lst[sensor],
                surge_ratio_range_lst[sensor],
                normal_mean_range_lst[sensor],
                normal_std_lst[sensor],
            )
        else:
            simulate_surge = None

        # For sine wave, set baseline in for all time
        if ts_shape == "sinusoidal":
            sine_period, theta = get_wave_period(
                freq_lst[sensor],
                period_str=sine_oh_params["daily"]["period_str"],
                theta_start_str=sine_oh_params["daily"]["theta_start_str"],
            )
            print("Daily sine_period:", sine_period, ", theta:", theta)
            anomaly_label["value"] = gen_cosine_imperfect(
                len(anomaly_label),
                sine_period=sine_period,
                theta=theta,
                sine_mean=sine_oh_params["daily"]["sine_mean"],
                amplitude=sine_oh_params["daily"]["amplitude"],
                sinebkpt_factor=sine_oh_params["daily"]["sinebkpt_factor"],
                noise_dist=sine_oh_params["daily"]["noise_dist"],
                noise_min=sine_oh_params["daily"]["noise_min"],
                noise_max=sine_oh_params["daily"]["noise_max"],
                sigma=sine_oh_params["daily"]["sigma"],
            )
        else:  # step function like
            # Simulate the mean values during normal times for each time-series
            num_days = anomaly_label["date"].nunique()
            normal_means_array = uniform(
                normal_mean_range[0], normal_mean_range[1], num_days
            )

        if add_season_trend:
            sine_period_year, theta_year = get_wave_period(
                freq_lst[sensor],
                period_str=sine_oh_params["yearly"]["period_str"],
                theta_start_str=sine_oh_params["yearly"]["theta_start_str"],
            )
            print(
                "Seasonal year sine_period:", sine_period_year, ", theta:", theta_year
            )
            anomaly_label["value"] += gen_cosine_trend(
                len(anomaly_label),
                sine_period=sine_period_year,
                theta=theta_year,
                sine_mean=sine_oh_params["yearly"]["sine_mean"],
                amplitude=sine_oh_params["yearly"]["amplitude"],
                sinebkpt_factor=sine_oh_params["yearly"]["sinebkpt_factor"],
                noise_dist=sine_oh_params["yearly"]["noise_dist"],
                noise_min=sine_oh_params["yearly"]["noise_min"],
                noise_max=sine_oh_params["yearly"]["noise_max"],
                sigma=sine_oh_params["yearly"]["sigma"],
                trendbkpt_factor=sine_oh_params["yearly"]["trendbkpt_factor"],
                start_coeff=sine_oh_params["yearly"]["start_coeff"],
                coeff_dist=sine_oh_params["yearly"]["coeff_dist"],
                coeff_max=sine_oh_params["yearly"]["coeff_max"],
                coeff_min=sine_oh_params["yearly"]["coeff_min"],
            )

        ret_df = pd.DataFrame()
        gb_dic = dict(list(anomaly_label.reset_index().groupby("date")))
        for i, (date, sub_df) in enumerate(gb_dic.items()):

            # Check if date is a weekend,
            if date.weekday() in [5, 6]:
                sub_ret_df = sub_df.copy()

                if ts_shape == "sinusoidal":  # downish concave trend during weekend
                    sub_ret_df["value"] += gen_pw_concave_trend(
                        len(sub_ret_df),
                        trendbkpt_factor=sine_oh_params["weekend"]["trendbkpt_factor"],
                        concavity=sine_oh_params["weekend"]["concavity"],
                        buffer_itval=sine_oh_params["weekend"]["buffer_itval"],
                        start_coeff=sine_oh_params["weekend"]["start_coeff"],
                        coeff_dist=sine_oh_params["weekend"]["coeff_dist"],
                        coeff_val=sine_oh_params["weekend"]["coeff_val"],
                        coeff_mid_factor=sine_oh_params["weekend"]["coeff_mid_factor"],
                        trend_shift_max=sine_oh_params["weekend"]["trend_shift_max"],
                        scale_2ndhalf_zero=sine_oh_params["weekend"][
                            "scale_2ndhalf_zero"
                        ],
                        date=date,
                        trend_val_max=sine_oh_params["weekend"]["trend_val_max"],
                    )
                else:  # For step-function profile, generate zero values if it is
                    sub_ret_df["value"] = 0

            else:
                on_hours, off_hours = (
                    sub_df[~sub_df["isOffHour"]],
                    sub_df[sub_df["isOffHour"]],
                )

                if ts_shape == "sinusoidal":
                    if not off_hours.empty:  # Slightly downward trend during off hours
                        off_hours["value"] += gen_pw_concave_trend(
                            len(off_hours),
                            trendbkpt_factor=sine_oh_params["ohweek"][
                                "trendbkpt_factor"
                            ],
                            concavity=sine_oh_params["ohweek"]["concavity"],
                            buffer_itval=sine_oh_params["ohweek"]["buffer_itval"],
                            start_coeff=sine_oh_params["ohweek"]["start_coeff"],
                            coeff_dist=sine_oh_params["ohweek"]["coeff_dist"],
                            coeff_val=sine_oh_params["ohweek"]["coeff_val"],
                            coeff_mid_factor=sine_oh_params["ohweek"][
                                "coeff_mid_factor"
                            ],
                            trend_shift_max=sine_oh_params["ohweek"]["trend_shift_max"],
                            scale_2ndhalf_zero=sine_oh_params["ohweek"][
                                "scale_2ndhalf_zero"
                            ],
                            date=date,
                            trend_val_max=sine_oh_params["ohweek"]["trend_val_max"],
                        )

                else:  # step-function like
                    # Generate zero values for weekday off-hour
                    if not off_hours.empty:
                        off_hours["value"] = 0
                    # Generate an array from normal distribution for weekday on-hour
                    if not on_hours.empty:
                        on_hours["value"] = normal(
                            normal_means_array[i], normal_std, on_hours.shape[0]
                        )

                sub_ret_df = pd.concat([off_hours, on_hours])
            ret_df = ret_df.append(sub_ret_df)
        ret_df = ret_df.sort_values("Timestamp").set_index("Timestamp")

        if simulate_surge:
            surge_occurrence = len(surge_start_end_indices)
            # Simulate surge degree for current time-series
            surge_ratios = uniform(
                surge_ratio_range[0], surge_ratio_range[1], surge_occurrence
            )
            surge_df = pd.DataFrame()
            # Indicate whether to allow absence of telemetry surge during anomalies from simulated anomaly label
            # comb_lst = []
            # for i in range(1, surge_occurrence+1):
            #     comb_lst = comb_lst + list(itertools.combinations(range(surge_occurrence), i))
            comb_lst = list(range(surge_occurrence))

            # Generate telemetries during surge
            for j in range(surge_occurrence):
                # tmp_surge_lst = np.random.choice(np.array(comb_lst, dtype=object), 1)[0]
                tmp_surge_lst = comb_lst
                surge_ratio = surge_ratios[j] if j in tmp_surge_lst else 1
                tmp_surge_df = anomaly_label.loc[
                    surge_start_end_indices[j][0] : surge_start_end_indices[j][1]
                ]
                tmp_surge_df["value"] = normal(
                    normal_means_array.mean() * surge_ratio,
                    normal_std,
                    tmp_surge_df.shape[0],
                )
                surge_df = surge_df.append(tmp_surge_df)

        if surge_with_decay:
            surge_occurrence = len(surge_start_end_indices)
            surge_df = pd.DataFrame()
            list_surge_or_dip = np.random.choice(["surge", "dip"], surge_occurrence)
            # Generate telemetries during surge
            for j in range(surge_occurrence):
                tmp_surge_df = anomaly_label.loc[
                    surge_start_end_indices[j][0] : surge_start_end_indices[j][1]
                ]
                ans = gen_beta_anom(
                    ret_df.loc[tmp_surge_df.index, "value"],
                    a=2,
                    b=5,
                    scale_fac=0.5,
                    surge_or_dip=list_surge_or_dip[j],
                )
                tmp_surge_df["value"] = ans
                surge_df = surge_df.append(tmp_surge_df)

        if simulate_surge or surge_with_decay:
            # Update time-series during normal time with the one with surges
            ret_df.loc[surge_df.index, "value"] = surge_df["value"]

        ret_df_lst.append(ret_df)
        print(f"\nSensor {sensor} Time-series Simulation Done.")
    return ret_df_lst


def populate_flow_all_nodes(gd=None, supply_mat=None) -> np.array:
    """
    Based on the simulation of selected nodes, populate the rest according to topology flow top-down

    Parameters
    ----------
    gd : an instance of GraphDataset object,
        GraphDataset
    supply_mat : supply matrix, np.array,
        stacked vertically by each the supply vector per timestamp (for each vertical vector only simulated nodes have meaningful entry otherwise filled with 0),
        or to be considered as a horizontal stack of time-series simulated per node (only simulated node have meaningful row of time-series, otherwise filled with row of zeros).

    Return
    ----------
    sln_mat : Solution matrix, np.array,
        stacked vertically by each the supply vector per timestamp, or to be considered as a horizontal stack of time-series simulated per node.
    """
    tmp_G = gd.G.copy()
    # Refine topology with relationship used for topology flow
    sub_topo_df = gd.topo_df[gd.topo_df["relationshipName"] != gd.relationship_to_flow]
    tmp_G.remove_edges_from(list(zip(sub_topo_df["sourceId"], sub_topo_df["targetId"])))
    # adj_mat A[i,j]: adjency matrix of graph, where a[i,j]=1 if there is an arc from i to j else 0
    adj_mat = nx.to_numpy_array(tmp_G)
    # Get factors to divide flows equally
    div_factor = np.sum(adj_mat, axis=1)
    div_factor[div_factor == 0] = 1
    # Solve linear equations of form: A_matrix x_matrix = supply_matrix, where A=I-(adj_mat)^T/div_factor
    A = np.eye(len(adj_mat)) - np.transpose(adj_mat) / div_factor
    sln_mat = np.linalg.solve(A, supply_mat).round(3)
    # print(f'Supply Matrix = {supply_mat}, Shape {supply_mat.shape}\nSolution Matrix = {sln_mat}, Shape {sln_mat.shape}')
    return sln_mat


def get_full_ts_df(
    sln_mat=None,
    time_series_index=None,
    gd=None,
    key_name=None,
    missing_ratio=0,
    value_noise=True,
    bump_up_neg=False,
    accept_neg=False,
    timestamp_noise=False,
) -> pd.DataFrame:
    """
    Get simulated telemetry time-series dataframe for all nodes in graph.

    Parameters
    ----------
    sln_mat : solution matrix obtained from function populate_flow_all_nodes(),
        np.array
    time_series_index : time-series index to put on the output dataframe,
        pandas.core.indexes.datetimes.DatetimeIndex
    gd : an instance of GraphDataset object,
        GraphDataset
    key_name : name of key (e.g. channel of sensor)
        str, (e.g. 'Amps.Ia')
    missing_ratio : percentage of missing value of time-series,
        float, default=0
    value_noise : indicate whether to add noise to the simulated telemetry,
        bool, default=True
    bump_up_neg, boolean: whether to bump up the time-series if there are negative values generated by mistake,
    accept_neg, boolean: whether to accept negative values, if false negative values are set to 0
    timestamp_noise : indicate whether to add noise (e.g. fractions of seconds) into timestamp,
        bool, default=False

    Return
    ----------
    ts_df : simulated continuous time-series dataframe for all nodes in graph with columns=['Timestamp', 'Id', 'Value', 'Key'],
        pd.DataFrame
    """
    ts_df = pd.DataFrame(sln_mat).T
    ts_df.index, ts_df.columns = time_series_index, gd.G.nodes

    # Add disturbance to value
    min_val = ts_df.min().min()
    if value_noise and bump_up_neg and min_val < 0:
        for col in ts_df.columns:
            ts_df[col] = ts_df[col].apply(lambda x: add_value_noise(x, accept_neg=True))
            ts_df[col] += abs(min_val)
    elif value_noise:
        for col in ts_df.columns:
            ts_df[col] = ts_df[col].apply(
                lambda x: add_value_noise(x, accept_neg=accept_neg)
            )

    # Pivot table from wide to long
    ts_df = pd.melt(
        ts_df.reset_index(), id_vars=["Timestamp"], var_name="Id", value_name="Value"
    )
    ts_df["Key"] = key_name
    # Randomly remove rows to simulate missings
    ts_df = random_drop_rows(ts_df, missing_ratio)
    # Add noise such as fractions of seconds to timestamp
    if timestamp_noise:
        ts_df = add_timestamp_noise(ts_df)
    ts_df = ts_df.sort_values("Timestamp").reset_index(drop=True)
    return ts_df


def get_cont_ts_df(
    unique_anomaly_label=True,
    anomaly_label_lst=None,
    surge_start_end_indices_lst=None,
    simulate_surge_lst=None,
    surge_ratio_range_lst=None,
    normal_mean_range_lst=None,
    normal_std_lst=None,
    gd=None,
    ts_shape=None,
    add_season_trend=None,
    sine_oh_params=None,
    freq_lst=None,
    surge_with_decay=True,
    bump_up_neg=False,
    accept_neg=False,
    key_name=None,
    missing_ratio=0,
    value_noise=True,
    timestamp_noise=False,
) -> pd.DataFrame:
    """
    Main function to simulate continuous telemetry time-series based on anomaly labels.

    Parameters
    ----------
    Other params refer to each function called within.

    Return
    ----------
    cont_ts_df : simulated time-series dataframe for all nodes in graph with columns=['Timestamp', 'Id', 'Value', 'Key'],
        pd.DataFrame
    """
    ret_df_lst = simulate_source_nodes_ts(
        unique_anomaly_label=unique_anomaly_label,
        anomaly_label_lst=anomaly_label_lst,
        surge_start_end_indices_lst=surge_start_end_indices_lst,
        simulate_surge_lst=simulate_surge_lst,
        surge_ratio_range_lst=surge_ratio_range_lst,
        normal_mean_range_lst=normal_mean_range_lst,
        normal_std_lst=normal_std_lst,
        gd=gd,
        ts_shape=ts_shape,
        add_season_trend=add_season_trend,
        sine_oh_params=sine_oh_params,
        freq_lst=freq_lst,
        surge_with_decay=surge_with_decay,
    )

    # Convert simulated time-series into supply matrix
    supply_mat = np.zeros((len(gd.G), ret_df_lst[0].shape[0]))
    for i, node in enumerate(gd.simulated_nodes):
        node_idx = list(gd.G.nodes).index(node)
        supply_mat[node_idx] = ret_df_lst[i]["value"]

    # Use the telemetry simulated for source nodes to populate the rest
    sln_mat = populate_flow_all_nodes(gd=gd, supply_mat=supply_mat)
    # Prepare time-series table for the system
    cont_ts_df = get_full_ts_df(
        sln_mat=sln_mat,
        time_series_index=ret_df_lst[0].index,
        key_name=key_name,
        gd=gd,
        missing_ratio=missing_ratio,
        value_noise=value_noise,
        bump_up_neg=bump_up_neg,
        accept_neg=accept_neg,
        timestamp_noise=timestamp_noise,
    )

    return cont_ts_df
