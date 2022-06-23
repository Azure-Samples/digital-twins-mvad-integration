#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
import os
import sys
from pathlib import Path
import yaml
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:80% !important; }</style>"))
sys.path.append(str(Path(os.getcwd()).parent) + '\src')
from graph_dataset import GraphDataset
from simulation_anomalylabels import simulate_anomaly_labels
from simulation_continuous import get_cont_ts_df
from simulation_categorical import get_cat_ts_df
from simulation_monotonic import get_monotonic_ts_df
from simulation_binary import get_binary_ts_df
from pattern_anomalies import get_pattern_anomalies
from data_history_formatter import data_history_formatter
from utils.utils_data_generation import generate_relationship_json, plot_ts

def main(
    experiment_name=None, \
    profiles_included = ['continuous', 'categorical', 'monotonic', 'binary'], \
    plot=False, \
    save=True, \
    timestamp_noise=False, \
    seed=2022, \
    init_graph_kwargs=None, \
    simulate_anomaly_labels_kwargs=None, \
    simulate_ts_kwargs_continuous=None, \
    simulate_ts_kwargs_categorical=None, \
    simulate_ts_kwargs_monotonic=None, \
    simulate_ts_kwargs_binary=None, \
    data_history_format=True
    ) -> None:
    """
    Main function for synthetic data generation.

    Parameters
    ----------
    experiment_name: name of experiment,
        str
    profiles_includes: list of data profiles included in synthetic data,
        list of str, default=['continuous', 'categorical', 'monotonic', 'binary']
    plot: whether to make plots when running the script,
        bool, default=False
    save: whether to save simulated data,
        bool, default=True
    timestamp_noise : indicate whether to add noise (e.g. fractions of seconds) into timestamp,
        bool, default=False
    seed : random seed to reproduce results,
        int, default=2022
    init_graph_kwargs: keyword arguments for initiate graph object,
        dict
    simulate_anomaly_labels_kwargs: keyword arguments for anomaly label generation,
        dict
    simulate_ts_kwargs_{data_profile}: keyword arguments for synthetic data generation of each individual data profile,
        dict
    data_history_format : indicate whether to format the data as the same as ADT Data History,
        bool, default=True

    Return
    ----------
    None
    """
    data_path = f'../data/synthetic_data/{experiment_name}/'
    if save:
        Path(data_path).mkdir(parents=True, exist_ok=True)
    np.random.seed(seed)

    # ## Chapter 1. Graph Object Creation
    # #### Step 1.1. Ingest Topology Table
    # Create a sample topology table
    with open(init_graph_kwargs['topo_json_file'], 'r') as f:
        topo_json = yaml.safe_load(f)
    topo_df = pd.DataFrame(list(topo_json.values())[0])
    if save:
        topo_df.to_csv(data_path + f'topology_{experiment_name}.csv', index=False)

    # #### Step 1.2. Convert Tabular Topology into Graph
    # Instantiate a GraphDataset object with topology given
    gd = GraphDataset(topo_df=topo_df, \
                      relationship_to_flow=init_graph_kwargs['relationship_to_flow'], \
                      simulated_nodes=init_graph_kwargs['simulated_nodes'])
    # Plot topology graph
    # if plot:
    #     gd.plot_graph()           

    # #### Step 1.3. Read in DTDL Models
    model_json_dic = {}
    for i in os.listdir(init_graph_kwargs['models_json_folder']):
        with open(os.path.join(init_graph_kwargs['models_json_folder'], i), 'r') as f:
            model_json = yaml.safe_load(f)
        model_name = model_json['displayName']
        model_json_dic[model_name] = model_json


    # ## Chapter 2. Anomaly Labels Simulation
    anomaly_label_lst, surge_start_end_indices_lst = simulate_anomaly_labels(**simulate_anomaly_labels_kwargs)
    if save:
        for i in range(simulate_anomaly_labels_kwargs['num_simulated_anomaly_ts']):
            anomaly_label_lst[i].reset_index().to_csv(data_path + f'anomaly_label_{experiment_name}_{i}.csv', index=False)


    # ## Chapter 3. Synthetic Data Simulation
    # ### Part 3.1. Data Profile - Continuous
    # Simulate Telemetry Time-series with Anomalies: 
    # - First Simulate Time-series for Selected Source Nodes
    # - Then Populate Time-series for the Rest Nodes from Topology Flow Top-down
    update_stream = pd.DataFrame()
    if 'continuous' in profiles_included:
        update_stream_continuous = pd.DataFrame()
        for i in range(len(simulate_ts_kwargs_continuous['key_name_lst'])):
            tmp_ts_df = get_cont_ts_df(
                unique_anomaly_label=simulate_ts_kwargs_continuous['unique_anomaly_label'], \
                anomaly_label_lst=anomaly_label_lst, \
                surge_start_end_indices_lst=surge_start_end_indices_lst, \
                simulate_surge_lst=simulate_ts_kwargs_continuous['simulate_surge_lst_lst'][i], \
                surge_ratio_range_lst=simulate_ts_kwargs_continuous['surge_ratio_range_lst_lst'][i], \
                normal_mean_range_lst=simulate_ts_kwargs_continuous['normal_mean_range_lst_lst'][i], \
                normal_std_lst=simulate_ts_kwargs_continuous['normal_std_lst_lst'][i], \
                gd=gd, \
                key_name=simulate_ts_kwargs_continuous['key_name_lst'][i], \
                missing_ratio=simulate_ts_kwargs_continuous['missing_ratio_lst'][i], \
                value_noise=simulate_ts_kwargs_continuous['value_noise_lst'][i], \
                timestamp_noise=simulate_ts_kwargs_continuous['timestamp_noise_lst'][i], \
                surge_with_decay=simulate_ts_kwargs_continuous['surge_with_decay_lst'][i])
            update_stream_continuous = pd.concat([update_stream_continuous, tmp_ts_df])
        update_stream_continuous['ModelId'] = np.nan
        for model_name, twins in init_graph_kwargs['model_twins_dic'].items():
            update_stream_continuous.loc[update_stream_continuous['Id'].isin(twins), 'ModelId'] = model_json_dic[model_name]['@id']
        update_stream_continuous = update_stream_continuous[['Id', 'ModelId', 'Key', 'Timestamp', 'Value']].sort_values(['Timestamp', 'Id', 'Key']).reset_index(drop=True)
        update_stream = pd.concat([update_stream, update_stream_continuous])

        if plot:
            plot_ts(ts_df=update_stream_continuous, anomaly_label=anomaly_label_lst[0])

        # Output Update_stream_continuous.csv, Topology_continuous.json & Topology_continuous.csv
        generate_relationship_json(
            topo_df=gd.topo_df, 
            save=save, 
            output_data_path=data_path, 
            output_json_file_name=f'topology_continuous_{experiment_name}.json',
            output_csv_file_name=f'topology_continuous_{experiment_name}.csv'
            )
        if save:
            update_stream_continuous.to_csv(data_path + f'update_stream_continuous_{experiment_name}.csv', index=False)
        print('Sample Update_stream_continuous.csv:')
        display(update_stream_continuous.head())


    # # Add Pattern Anomalies
    # timerange_str_lst = [['2021-01-07 11:00:00', '2021-01-07 14:00:00'], ['2021-01-08 11:00:00', '2021-01-08 14:00:00']]
        
    # update_stream_continuous = get_pattern_anomalies(update_stream_continuous, 'C', 'Amps_Ia', timerange_str_lst, ['multiple' for _ in range(len(timerange_str_lst))], [10 for _ in range(len(timerange_str_lst))])
    # update_stream_continuous = get_pattern_anomalies(update_stream_continuous, 'D', 'Amps_Ia', timerange_str_lst, ['multiple' for _ in range(len(timerange_str_lst))], [10 for _ in range(len(timerange_str_lst))])
    # update_stream_continuous = get_pattern_anomalies(update_stream_continuous, 'E', 'Amps_Ia', timerange_str_lst, ['multiple' for _ in range(len(timerange_str_lst))], [1/100 for _ in range(len(timerange_str_lst))])

    # if plot:
    #     plot_ts(ts_df=update_stream_continuous, \
    #             anomaly_label=anomaly_label_lst[0])


    # ### Part 3.2. Data Profile - Categorical
    # Simulate Categorical Time-series (e.g. PowerLevel as one of 'High'/'Mid'/'Low' for devices A & B)
    if 'categorical' in profiles_included:
        update_stream_categorical = get_cat_ts_df(
            anomaly_label_lst=anomaly_label_lst,
            num_simulated_ts=simulate_ts_kwargs_categorical['num_simulated_ts'],
            freq_lst=simulate_ts_kwargs_categorical['freq_lst'],
            id_name_lst=simulate_ts_kwargs_categorical['id_name_lst'],
            key_name_lst=simulate_ts_kwargs_categorical['key_name_lst'],
            cat_names_lst=simulate_ts_kwargs_categorical['cat_names_lst'],
            cat_ratio_lst=simulate_ts_kwargs_categorical['cat_ratio_lst'],
            missing_ratio_lst=simulate_ts_kwargs_categorical['missing_ratio_lst'],
            timestamp_noise_lst=simulate_ts_kwargs_categorical['timestamp_noise_lst']
            )
        update_stream_categorical['ModelId'] = np.nan
        for model_name, twins in init_graph_kwargs['model_twins_dic'].items():
            update_stream_categorical.loc[update_stream_categorical['Id'].isin(twins), 'ModelId'] = model_json_dic[model_name]['@id']
        update_stream_categorical = update_stream_categorical[['Id', 'ModelId', 'Key', 'Timestamp', 'Value']].sort_values(['Timestamp', 'Id', 'Key']).reset_index(drop=True)
        update_stream = pd.concat([update_stream, update_stream_categorical])
        
        if plot:
            plot_ts(ts_df=update_stream_categorical,
                    anomaly_label=anomaly_label_lst[0], 
                    mode='markers')
            display(update_stream_categorical)
                
        # Output Update_stream_categorical.csv
        if save:
            update_stream_categorical.to_csv(data_path + f'update_stream_categorical_{experiment_name}.csv', index=False)
        print('Sample Update_stream_categorical.csv:')
        display(update_stream_categorical.head())


    # ### Part 3.3. Data Profile - Monotonic
    # Simulate Monotonic Time-series based on Continuous Time-series
    if 'monotonic' in profiles_included:
        update_stream_monotonic  = pd.DataFrame()
        for i in range(len(simulate_ts_kwargs_monotonic['key_name_lst'])):
            tmp_monotonic_ts_df = get_monotonic_ts_df(
                unique_anomaly_label=simulate_ts_kwargs_monotonic['unique_anomaly_label'],
                anomaly_label_lst=anomaly_label_lst,
                surge_start_end_indices_lst=surge_start_end_indices_lst, 
                simulate_surge_lst=simulate_ts_kwargs_monotonic['simulate_surge_lst_lst'][i],
                surge_ratio_range_lst=simulate_ts_kwargs_monotonic['surge_ratio_range_lst_lst'][i],
                normal_mean_range_lst=simulate_ts_kwargs_monotonic['normal_mean_range_lst_lst'][i],
                normal_std_lst=simulate_ts_kwargs_monotonic['normal_std_lst_lst'][i],
                gd=gd,
                key_name=simulate_ts_kwargs_monotonic['key_name_lst'][i],
                missing_ratio=simulate_ts_kwargs_monotonic['missing_ratio_lst'][i],
                value_noise=simulate_ts_kwargs_monotonic['value_noise_lst'][i],
                timestamp_noise=simulate_ts_kwargs_monotonic['timestamp_noise_lst'][i]
                )
            update_stream_monotonic = pd.concat([update_stream_monotonic, tmp_monotonic_ts_df])
        update_stream_monotonic['ModelId'] = np.nan
        for model_name, twins in init_graph_kwargs['model_twins_dic'].items():
            update_stream_monotonic.loc[update_stream_monotonic['Id'].isin(twins), 'ModelId'] = model_json_dic[model_name]['@id']
        update_stream_monotonic = update_stream_monotonic[['Id', 'ModelId', 'Key', 'Timestamp', 'Value']].sort_values(['Timestamp', 'Id', 'Key']).reset_index(drop=True)
        update_stream = pd.concat([update_stream, update_stream_monotonic])

        if plot:
            plot_ts(ts_df=update_stream_monotonic,
                    anomaly_label=anomaly_label_lst[0])
            display(update_stream_monotonic)

        # Output Update_stream_monotonic.csv, Topology_monotonic.json & Topology_monotonic.csv
        generate_relationship_json(
            topo_df=gd.topo_df, 
            save=save, 
            output_data_path=data_path, 
            output_json_file_name=f'topology_monotonic_{experiment_name}.json',
            output_csv_file_name=f'topology_monotonic_{experiment_name}.csv'
            )
        if save:
            update_stream_monotonic.to_csv(data_path + f'update_stream_monotonic_{experiment_name}.csv', index=False)
        print('\nSample Update_stream_monotonic.csv:')
        display(update_stream_monotonic.head())

    # ### Part 3.4. Data Profile - Binary
    # Simulate Binary Time-series
    if 'binary' in profiles_included:
        update_stream_binary = get_binary_ts_df(**simulate_ts_kwargs_binary)
        print('Binary count for each Id:')
        print(update_stream_binary.groupby('Id')['Value'].value_counts())
        update_stream_binary['ModelId'] = np.nan
        for model_name, twins in init_graph_kwargs['model_twins_dic'].items():
            update_stream_binary.loc[update_stream_binary['Id'].isin(twins), 'ModelId'] = model_json_dic[model_name]['@id']
        update_stream_binary = update_stream_binary[['Id', 'ModelId', 'Key', 'Timestamp', 'Value']].sort_values(['Timestamp', 'Id', 'Key']).reset_index(drop=True)
        update_stream = pd.concat([update_stream, update_stream_binary])

        if plot:
            plot_ts(ts_df=update_stream_binary,
                    anomaly_label=anomaly_label_lst[0])
            display(update_stream_binary)

        # Output: Update_stream_binary.csv
        if save:
            update_stream_binary.to_csv(data_path + f'update_stream_binary_{experiment_name}.csv', index=False)
        print('Sample Update_stream_binary.csv:')
        display(update_stream_binary.head(10))

    # ### Part 3.5. Combine Different Data Profile of Same Timerange Together
    if not update_stream.empty:
        update_stream = update_stream.sort_values('Timestamp').reset_index(drop=True)
        if plot:
            plot_ts(ts_df=update_stream, \
                    anomaly_label=anomaly_label_lst[0])
        if save:
            update_stream.to_csv(data_path + f'update_stream_{experiment_name}.csv', index=False)
            print('Sample Update_stream.csv:')
            display(update_stream.head(10))

    # ### Part 3.6. Get Initial Twins
    if not update_stream.empty:
        initial_df = update_stream.loc[update_stream.groupby(['Id', 'Key'])['Timestamp'].idxmin()].reset_index(drop=True)
        initial_df = initial_df[['Id', 'ModelId', 'Key', 'Timestamp', 'Value']]\
                                .sort_values(['Timestamp', 'Id', 'Key']).reset_index(drop=True)
        if save:
            initial_df.to_csv(data_path + f'initial_twins_{experiment_name}.csv', index=False)
        print('Initial_twins.csv:')
        display(initial_df)

    # ### Part 4. Format Synthetic Data to get consistent with ADT Data History
    if data_history_format:
        update_stream_dh = data_history_formatter(df=update_stream)
        update_stream_dh.to_csv(data_path + f'update_stream_dh_{experiment_name}.csv', index=False)
        print('Sample Update_stream.csv In ADT Data History Format:')
        display(update_stream_dh.head(10))


if __name__ == '__main__':
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)

    main(**config)
