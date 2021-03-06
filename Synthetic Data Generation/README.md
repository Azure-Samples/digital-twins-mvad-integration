# Intro

This folder provides a synthetic data generation pipeline, to generate synthetic time-series data to mimic a network of sensors, build ADT twins around them and accordingly update their properties, representing the sensor data. With sensor data flowing through the provisioned ADT twin properties, we can mimic the typical customer data-flow, i.e. IoT sensor data egressed from ADT to ADX, Azure Data Explorer (potentially through an [ADT Data History connection](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-use-data-history?tabs=cli); alternatively, the time-series can be uploaded to a  linked ADX table). From the ADX table, the sensor data feeds into the ADT-MVAD integration toolkit, which contains preprocessing layers and downstream AI training and inference modules for multivariate anomaly detection.

Note that the synthetic data generation pipeline additionally provides generated labels of anomalies. The labels can optionally be used to validate the performance of the MVAD ML task, with the optional code provided in the `MVAD_Visualization.ipynb` notebook. 

<br>


# Synthetic Data Generation

## To run
Follow this 5-step process:
- Step1: Install necessary packages: `pip install -r requirements.txt` 
- Step2: Define DTDL models in the folder `./data/models_json/`.
- Step3: Provide twin graph topology in the folder `./data/topology_json/`.
- Step4: Specify configurations in the yaml file `./src/config.yaml`.
- Step5: Run the main file: `python ./src/main.py`.

<br>

Under the hood the main file calls the following individual components which are modularized in the following files:
- `./src/graph_dataset.py` contains the class to generate graph object.
- `./src/simulation_anomalylabels.py` contains the function to simulate anomaly labels.
- `./src/simulation_binary.py` contains the function to simulate binary time-series.
- `./src/simulation_categorical.py` contains the function to simulate categorical time-series.
- `./src/simulation_continuous.py` contains the function to simulate continuous time-series.
- `./src/simulation_monotonic.py` contains the function to simulate monotonic time-series.
- `./src/pattern_anomalies.py` contains the function to add on pattern anomalies to the time-series simulated.

Additionally, the folder also provides:
- An illustrative data generation notebook with graphs and plots: `./notebooks/Synthetic Data Simulation with Graph-Demo.ipynb`.
- `./src/data_history_formatter.py` contains the function to format the data generated as the same as ADT Data History.
- `./src/utils/utils_data_generation.py` contains the helper functions.

<br>

## How it works

To produce synthetic data:

1. First define the topology table of assets or machines that will make up your system's graph, i.e. your ADT instance. We use edge definition to determine the graph, i.e. define the `sourceId` and `targetId` and `relationshipName`.
2. Build a networkX graph from the defined topology. Determine which nodes of the graph are the ultimate source nodes that we will actually apply simulation, while the rest, considered as feed nodes, whose telemetry will be generated by the "flow" top-down via the selected topology.
3. Generate anomalies, i.e. anomaly label time-series (1 when anomalous otherwise 0) and the surge ratio (factor by which typical value is multiplied): we define the start and end time of the entire anomalies series, the frequency of updating or sample rate, the start of end time of each weekday as on/off-hours. Also we specify the number of anomaly periods and the surge duration as well as surge degree of anomaly periods, all of which will be uniformly sampled from corresponding ranges. The surges are akin to a step-function up.
4. Simulate telemetry time-series for all nodes in the graph:
   - First, for a single numerical telemetry, build "normal" time-series for the source nodes, which assumes a *monday-friday business hours pattern*, with on and off-hours:
     - For **step-like** profile: at the *off-hours*, the values are mostly zero (initially set as zero but with options to introduce minor disturbance), and normal distributed at the *on-hours* when in the absence of anomalies.    
   - Then, add on the anomalies simulated from the last step. This constitutes the supply matrix for the network flow into the system.
   - Using the graph, get the adjacency matrix. Additionally, compute a "dividing-factor" matrix which weighs the flow from the source nodes into target nodes, we hereby assume that the flow is divided equally. Using these 2 matrices, we can solve the system of linear equations to get the flow for all the inner nodes.
   - Obtain a table of simulated telemetry with multiple options available to add-on, e.g. whether to include missings or random noises to telemetry or timestamp.
   - May repeat the last three steps to simulate multiple telemetries, with different value mean for the "normal" pattern and/or different generated anomalies.
5. (Optional) Simulate categorical time-series for selected nodes, given the name of categorical property, updating frequency, selected nodes, list of all possible values of this categorical property and portion of each. Same options of missings or noises available. The simulated categorical time-series table could be combined with the numerical/telemetry table simulated before as the output of update stream.
6. (Optional) Simulate monotonic numerical time-series for selected nodes, new continuous time-series will be first generated for this purpose, followed by a cumulative transformation applied to create monotonic telemetries (e.g. PowerMeter reading). The simulated monotonic time-series table could be combined with the numerical/telemetry table simulated before as the output of update stream.
7. (Optional) Simulate binary time-series for selected nodes (e.g. Switch On/Off). The simulated binary time-series table could be combined with the numerical/telemetry table simulated before as the output of update stream.
8. Get initial twin snapshot for each simulated twin which will be used for ADT initialization.
9. (Optional) Wrap up the data simulation process and format data according to ADT Data History, with columns 'TimeStamp', 'SourceTimeStamp', 'ServiceId', 'Id', 'ModelId', 'Key', 'Value', 'RelationshipTarget', 'RelationshipId'. 


Let's summarize the assumptions for this synthetic data generation:

- The normal pattern in the times series follows a monday-friday business hours pattern, where at the off-hours, the values are mostly zero, and normal distributed at the on-hours when in the absence of anomalies.
- The flow through the network of nodes depends on the ultimate supply source nodes, and flow thereafter is equally divided amongst the target nodes.
- Anomalies for each node, or time-series for the numerical or categorical properties, in the graph, are all aligned.

## Output files

The outputs of this synthetic data generation notebook, to be used in the next step of the pipeline, are as follows and stored at `./data/synthetic_data/{experiment_name}`:

- `topology_{data_profile}_{experiment_name}.csv` : Topology files for each data profile, which contain the relationships of which machine is connected to which. They contain `relationshipName`, `sourceId`, `targetId` as columns.
- `initial_twins_{experiment_name}.csv`: A file for twins' namings and initial properties (keys and corresponding values). It contains `Id`, `ModelId`, `Timestamp`, `Value` as columns.
- `update_stream_{data_profile}_{experiment_name}.csv`: Simulated data files for each data profile for the IoT telemetry signals, as generated above, to be used for the twin property updates. They contain `Id`, `Key`, `Timestamp`, `Value` as columns.
- `update_stream_{experiment_name}.csv`: Concatenated simulated data files for all data profile for the IoT telemetry signals, as generated above, to be used for the twin property updates. It contains `Id`, `Key`, `Timestamp`, `Value` as columns.
- `update_stream_dh_{experiment_name}.csv`: Format the comprehensive simulated data according to ADT Data History. It contains `TimeStamp`, `SourceTimeStamp`, `ServiceId`, `Id`, `ModelId`, `Key`, `Value`, `RelationshipTarget`, `RelationshipId` as columns.

Couple complementary files are also stored in the same folder for reference.

- `topology_{data_profile}_{experiment_name}.json` : Topology files for each data profile contain same info as `topology.csv` but translated into JSON format.
- `anomaly_label_{experiment_name}.csv` : A file for the anomaly label simulated. It contains `Timestamp`, `date`, `isAnomaly`, `isOffHour` as columns.

<br>

# ADT model, twin creation & properties update

## Prerequisites

- Provision an Azure Digital Twins instance, and get the URL endpoint. User needs to add on the *Azure Digitial Twins Data Owner* role.
   - Currently, ADT instance with SourceTimeStamp feature is only available in PPE and Canary. Make sure you set up the instance in those two regions in order to use the feature.
- pip install `azure.digitaltwins.core` Python package to use the ADT Python SDK, and `azure-identity` for AAD authentication.
   - The latest released `azure.digitaltwins.core` sdk only supports api version "2020-10-31". In order to update the twins with SourceTime metadata property, we need to use api "2021-06-30-preview". Clone the sdk [here](https://github.com/yizhu2/azure-sdk-for-python/tree/yizhu2/update_api), at the root folder, run the commands below to have the sdk with newest api installed in your local env.
      ```
      cd .\sdk\digitaltwins\azure-digitaltwins-core  
      python .\setup.py install
      ```
      To change the api version yourself, navigate to `\sdk\digitaltwins\azure-digitaltwins-core\azure\digitaltwins\core\_generated\_configuration.py` and `sdk\digitaltwins\azure-digitaltwins-core\azure\digitaltwins\core\_generated\aio\_configuration.py`, change the `api_version` config and reinstall the package.

Note: In subsequent step, to historize the twin property updates, set up an ADT Data History Connection, as well as provision the required EventHub and Kusto resources.

## To run

Run the python script ./src/create_update_twins.py or the notebook `./notebooks/adt_create_update.ipynb`, which has example output cells.

## How it works

To be able to pass on the generated time-series signal in `update_stream.csv`, we need to:

- Create client connection to ADT instance using its URL endpoint.
- Create and upload ADT models using DTDL language, making sure to include the properties for the time-series, and the graph relationships. For our synthetic data example, we create 2 models for the 2 types of machines, see the json files in  *./data/models_json*.
- Build ADT twins for the nodes in the graph, by referencing the correct model ids, and initializing the properties, as in `initial_twins.csv`.
- Add relationships to the ADT twins using the topology in `topology.csv`.
- Update properties as json patches using generated data in `update_stream.csv`.


# ADT data egress to Azure Data Explorer (ADX)
To send the twin time-series data to ADX, you can:
- Set up Data History in your ADT instance
- Upload data directly to an ADX table, as a shortcut to onboard quickly for the sample synthetic dataset dataset.

## [Option 1] Set up Data History in your ADT instance
ADT's Data History feature automatically historizes digital twin property updates from an ADT instance to an ADX cluster. This can be set up through Azure CLI and Azure portal, to connect the ADT instance to Event Hub and an ADX cluster.

Please see more details on how to set up ADT Data History connection [here](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-use-data-history?tabs=cli).


## [Option 2] Upload data directly to an ADX table

To upload the generated csv data to ADX:
- run the following command in ADX to first set new table's schema:
   > `.create table ['<Table Name>']  (['TimeStamp']:datetime, ['SourceTimeStamp']:datetime, ['ServiceId']:string, ['Id']:string, ['ModelId']:string, ['Key']:string, ['Value']:dynamic, ['RelationshipTarget']:string, ['RelationshipId']:string)`
- Right-click on the empty table and select `Import Data From Local Files` and be sure to check `Ignore first record`. The ADX table will now be ready to use for Synapse pipeline runs.