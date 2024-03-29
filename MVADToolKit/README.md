# About

This folder contains the ADT-MVAD toolkit, i.e.  a set of pipeline artifacts and a powershell script that will help provision necessary resources and link them together to Synapse workspace.

## Toolkit Structure
The toolkit contains the following:
- `linkedService/*`: Templates of linkedServices to your Synapse workspace. Values will be automatically populated by the setup script.
- `notebook/*`: Notebooks with Python code for
    - Preprocessing the raw data and format data to be taken in readily by MVAD, and optionally smoothen training data
    - Calling MVAD apis to run training or inference
    - Visualizing plots to interpret raw anomaly results
- `pipeline/*`: Synapse training/inference pipeline templates. These configurations are provided for the Synapse workflow: the training and inference pipelines to define scenarios through user-input, help orchestrate and pass meta-data between training and inference, and surface any errors.
- `setup.ps1`: Powershell script to provision resources, link resources to Synapse workspace, and grant necessary permissions.

<br>

## Dependencies
This toolkit currently implements MVAD version 1.1, as set in the SynapseML version: `com.microsoft.azure:synapseml_2.12:0.9.5-19-82d6b563-SNAPSHOT`, that runs in the Synapse pools.

<br>

# Setup: Provision Resources

Before setting up, see the prerequisites in the [main page](../README.md/#prerequisites).
To run `setup.ps1`, provide these parameters:

#### General setup parameters

* `SubscriptionId` [required]: The subscription where all the newly provisioned resources reside in.
* `ResourceGroup` [required]: The script will create new resource group.
* `Location` [required]: Region of the resource group.

#### ADX-related setup parameters

* `ADXSubscriptionID` [optional]: The subscription ID of your Data Hisotry ADX cluster. Default to the same subscription for all your resources (SubscriptionId in General section above).
* `ADXResourceGroup` [required]: ADX cluster resource group.
* `ADXEndpoint` [required]: ADX URI (obtained from ADX instance in Azure portal).
* `ADXClusterName` [required]: ADX cluster name.
* `ADXDatabaseName` [required]: ADX cluster database name.
* `ADXTable` [required]: ADX cluster table name used for ADT data history.

#### ADT-related setup parameters

* `ADTSubscriptionId` [optional]: The subscription ID of your ADT Instance. Default to the same subscription for all your resources (SubscriptionId in General section above).
* `ADTResourceGroup` [required]: ADT resource group.
* `ADTEndpoint` [required]: ADT host name (obtained from ADT instance in Azure portal).

#### Synapse-Sql-User setup parameters

* `SqlUser` [required]: Synapse workspace Sql user.
* `SqlPassword` [required]: Synapse workspace Sql password. The password must be atleast 8 characters long and contain characters from three of the following four categories: (uppercase letters, lowercase letters, digits (0-9), Non-alphanumeric characters such as: !, $, #, or %).

#### Default Resource Names [Optional]

* `SynapseWorkspaceName`: Synapse workspace name, defaults to `adt-synapse`.
* `KeyVaultName`: Key Vault name, defaults to `synapse-keyvault`.
* `MVADResourceName`: MVAD name, defaults to `adt-mvad`.
* `ADLSAccountName`: ADLS name, defaults to `synapseadls`.
* `ADLSContainer`: ADLS container name, defaults to `user`.

#### Default Resource Configuration [Optional]

* `SparkPoolNodeSize`: Synapse spark pool Node Size, defaults to `Small` (4 vCores / 32 GB memory).
* `AutoScaleMinNodeCount`: Minimum Synapse spark pool Node Count, defaults to `4`.
* `AutoScaleMaxNodeCount`: Maximum Synapse spark pool Node Count, defaults to `10`.
* `AutoPauseDelayInMinute`: Synapse spark pool autopausing policy, defaults to `5`.
* `NotebookNodeCount`: Synapse notebook Spark pool worker node allocation, defaults to `1`.

<br>

## Run setup script

Run the setup script with your resource parameters:

`.\setup.ps1 -SubscriptionId <subscriptionId> -ResourceGroup <rg> -Location <location> -ADXResourceGroup <adx_rg> -ADXEndpoint <adx_endpoint> -ADXClusterName <cluster> -ADXDatabaseName <database> -ADXTable <table> -ADTResourceGroup <adt_rg> -ADTEndpoint <adt_endpoint> -SqlUser <user> -SqlPassword <password> `

- This will run for 5-10 mins.
- If there are permission errors, use the following command:` Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`
- While the script runs, you can log to your Azure account and check the logs for progress. Once the script has finished running, a url to your **Synapse workspace** will be returned.

<br>

# User-guide 

The setup script provisions all necessary resources, including the Synapse workspace that we leverage for the toolkit. We perform the following steps through the Synapse UI:
1. Scenario definition: Define one scenario by filling in the required user-defined parameters in the parameter pop-up window
2. Training pipeline run: Trigger the training pipeline run for the defined scenario
3. Inference pipeline run: Trigger the inference pipeline run, using the generated inference trigger which shows up once training successfully completes. 
4. Repeat steps 1-3 as needed for your different scenarios
5. Monitor pipeline runs: Use the monitoring UI to ensure the runs complete without errors
6. Access & visualize results: Access the anomaly detection results in associated scenario's ADX table, and use the provided visualization notebook to get plots of the time-series and anomalies. 
7. Stopping inference pipeline runs as needed

The following data-flow diagram illustrates how the toolkit's provided pipeline configurations allows data to flow, from the user-input parameters to the training pipeline, to the inference pipeline, and finally as the anomaly detection results.

  ![Training-inf-dataflow](../media/Train-Inf-Data-flow.png)

Note:
- Before running multiple pipelines concurrently, make sure your resources have adequate capacity. In the troubleshooting guide, refer to the following sections:
  - [Concurrent read and write capacity of the ADX cluster](./troubleshooting.md#concurrent-read-and-write-capacity-of-adx-cluster)
  - [Synapse Spark pool nodes](./troubleshooting.md#spark-pool-nodes)
  - [Synapse notebook idle time](./troubleshooting.md#synapse-notebook-idle-time)

<br> 

## Scenario definition & training pipeline run
In the Synapse Workspace UI,
- navigate to the Pipelines tab, and click on the `consolidated_training_pipeline`.
- Click on the "Add trigger" button and click on the "Trigger now" button
    
    ![scenario-def-training](../media/scenario-def-training.png)

A pop up will appear, with a list of necessary scenario parameters to fill in. The parameters are as follows:
- `scenario_name`: identifier for the MVAD analysis you want to do, it can be representative of the context i.e. twin cluster and properties, and the specific parameters used.
- To provide context:
  - `customer_adt_query`: ADT query to identify ADT twins (within the linked ADT instance) to be monitored,  query must result in a list of dtid entries. The ADT query must be in the form ready to be executed, as in the [ADX plugin](https://docs.microsoft.com/en-us/azure/digital-twins/concepts-data-explorer-plugin). E.g.: `SELECT t.$dtId as tid FROM DIGITALTWINS t`
  - `relevant_twin_properties`: List of twin properties of interest.E.g. ["water_flow", "oil_flow"]
- To add the time-series information info: 
  - `adx_table`: By default, this is prefilled with the ADX table linked during the setup phase, and should contain the historized time-series data.
  - (optional) `adx_mapping_id`, `adx_mapping_key`, `adx_mapping_value`, `adx_mapping_sourcetime`. By default, this contains the prefilled values `Id`, `Key`, `Value`, `SourceTimestamp` respectively, as this the schema for an ADX table set up through [ADT's Data History connection](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-use-data-history?tabs=cli). If you are connecting an ADX table that is set up otherwise, please add the mapping to its schema, where `adx_mapping_id` refers to the column name of the column for the ADT twin ids, `adx_mapping_key`: column name for the ADT twin properties' names, `ad_mapping_value`: column name for the ADT twin properties' values, `adt_mapping_sourcetime`: column name for the timestamps of the ADT twin properties' value updates. This assumes that the ADX table is already in long format. 
- MVAD prep parameters:
  - `resampling_rate_min`: unit, in minutes, for realignment of the various properties/variable's timestamps during preprocessing. E.g. 1
  - `train_data_smoothing`: boolean (true/false) to denote whether to smooth the training data during preprocessing. This is recommended to remove outliers or univariate anomalies to capture normal behavior in the training data, [as recommended by MVAD](https://docs.microsoft.com/en-us/azure/cognitive-services/anomaly-detector/concepts/best-practices-multivariate). 
- MVAD parameters:
  - `sliding_window`: number of data-points used by the MVAD algorithm to determine an anomaly, ideally an adequate sliding window captures periodicity in the data. E.g. 1440 representing a sliding window of 1 day for data of 1-minute granularity
  - `train_start_datetime`, `train_end_datetime`: start and end datetime for training data. Ensure the datetime entered is in ISO8601 format, by default time taken as UTC time. E.g. 2022-06-01T00:00:00Z, 2022-06-14T00:00:00Z
  
    ![scenario-params](../media/scenario-params.png)

- Click on 'OK' to trigger the training pipeline.

Once the training pipeline is triggered, the pipeline run can be **monitored** using the "Pipeline runs" tab in the "Monitor" tab. The training pipeline run will appear, and you can drill down on the run by clicking on the name. It will open this page:

   ![training-pipeline-run](../media/training-pipeline-run.png)

Note: 
- The activities in the training pipeline runs are planned according to the configurations provided in the toolkit's pipeline json file.
- As part of the activities, the training notebook takes in the user-defined input parameters.
- The notebook can be opened using the button highlighted above, and can be used for **debugging** in case of pipeline failure.
 
<br> 


## Inference pipeline run
To check that a training pipeline run has finished without errors: 
- Go to “Monitor” tab --> “Pipeline runs” tab, and check the status.
- •Check that the training pipeline run’s metadata has been logged in the metadata-table in the linked ADX table. Check that your `scenario_name` appears in the result generated by this command: `metadataTable  | take 100 `

Once the training pipeline is finished executing without errors, an associated inference pipeline trigger is automatically generated, configured to run on a scheduled basis (1 run per 10 minutes to give anomaly detection result at near real-time). According to the toolkit's pipeline configuration:
- The automatically generated inference trigger is named: `scenario_name+ ”10min”`, and has a 10-minute recurrence inference cadence. This can be modified manually by the user, see [this section](./troubleshooting.md/#starting-stopping--creating-new-inference-pipeline-runs) in the troubleshooting guide.
-	The inference data’s time-window is dynamically calculated by the inference pipeline setup, through the equation below:
    >`Inf_window_start = T-(sliding_window * resampling_rate + inf_cadence_10min + buffer_5min)`
     ![inf_window_timeline](../media/inf_window_timeline.png)
    
    - The inference time-window is calculated from the user-input parameters of `sliding_window` (in terms of data-points) and `resampling_rate` such that the total inference time-window includes at least the **sliding window time**, and a **10min time period** for which the inference results will be extracted. 
    - A 5-minute buffer time is used to make sure data gets populated in ADX through data-history, particularly if it has batch lag.
- For each scenario, the necessary parameter values are carried on from the training phase to the inference phase. This includes: 
    - The Cognitive Services’ trained MVAD model id, which is critical to keep track of the scenario’s trained model that the inference pipeline calls upon
    - A list of essential pre-processing parameter values used at training that need to be persisted and used at inference, including:
    -	Resampling rate and associated aggregation functions for numerical and categorical variables (i.e. twin properties)
    - Normalization (min and max values) and value lists for respectively numerical and categorical variables (i.e. twin properties), as seen in training data
    - Note that most of these pre-processing parameters are currently default arguments in the pre-processing functions used in the training and inference notebooks, but can be tweaked by editing the notebooks.


To run the inference pipeline for a particular scenario, go to the associated trigger:
- navigate to the "Manage" tab, and click the "Trigger" tab.
- Select the wanted trigger from the list (recall scenario name is the first part of the trigger name) 
- Specify the start datetime, and optionally specify an end datetime.
- Select the status to be "started", click OK.

 ![inf-pipeline-start](../media/Synapse-trigger.png) 

<br> 

## Scenario & pipeline run monitoring
The past and present training and inference pipeline runs can be **monitored** through the Monitor UI in the “Pipeline runs” tab. Note that the pipeline names are “consolidated_inference_pipeline” and “consolidated_training_pipeline” as named in the artifact by default for any scenario.

### Verifying Scenario Parameters
- Currently, the associated scenario for each pipeline run can be identified by the parameters in the “Parameter” column, including **scenario name**. The parameters’ list contains all the parameters that are input for a particular scenario’s training or inference run.
- Additionally, scenario names for inference runs can currently be identified according to their dynamically-named automatic trigger: named according to `scenario_name+”10min”`. 

### Metadata table
Scenarios, for which training has succeeded, are also logged into the `metadataTable` ADX table in the ADX cluster database associated with this feature. It contains the necessary parameters for each trained scenario, including:
- Input parameters that define the scenario: training start-time, training end-time, sliding window, input ADT query, selected metrics or properties, resampling rate
- Training output parameters, used as input for the scenario’s associated inference pipeline: 
  - `mvadModelId`: this is the Cognitive Services’ trained MVAD model id, critical to keep track of the scenario’s trained model that the inference pipeline calls upon
  - `AdditionalNote`: a list of essential pre-processing parameter values used at training that needs to be persisted and used at inference, as described in section 4.2. 

 ![metadata-table](../media/metadata-table.png)

### Pipeline runs’ status and error logs
In the Monitor UI for pipeline runs, users can check their progress using the Status column for filter with options: “succeeded”, “in progress”, “queued”, “failed”, “cancelled”. For runs that have failed, users can:
- Check the error message in the Error column
- Drill down further where the error occurred in the notebook: Click on failed pipeline run to show the list of associated activity runs --> Open notebook snapshot --> Look for notebook cell generating the error.

<br> 

## Accessing & visualizing anomaly results
Anomaly detection results can be accessed in the ADX cluster used. Currently, 
- One result table is created per scenario, and takes the name of the scenario.
- Besides the anomaly results, the table consists of the preprocessed time-series of the selected properties of queried twins, with the preprocessing as done the pre-processing function in the notebooks.
- The anomaly results are in the `isAnomaly` and `result` columns, and are per timestamp of the processed data:
  - `isAnomaly`: Boolean giving raw anomaly result from MVAD inference.
  - `result`: list containing more detailed result scores, including (more information in [this MVAD documentation](https://docs.microsoft.com/en-us/azure/cognitive-services/anomaly-detector/tutorials/learn-multivariate-anomaly-detection)): 
    - `severity`: relative severity of the anomaly (from 0-normal- to 1). 
    - `score`: raw score from MVAD model
    - `contributors`: dictionary of contribution score per each variable, which can be used to drill into which variable or properties contributed most to the anomaly at that timestamp.

<br> 

### Visualizing anomaly results
To help visualize the anomaly detection results, you can have a first look through ADX. The following Kusto command can be used (substitute in for the right `scenario_name` and `param_name` for the wanted properties' names, or part of - e.g. `*_flow`):

```
<scenario_name>
| extend timestamp=todatetime(timestamp)
| project-keep timestamp, isAnomaly, *_<param_name>
| order by timestamp asc
| render timechart
```

Alternatively, you can use the provided optional visualization notebook:
![viz-notebook](../media/viz-notebook.png)
- In the Synapse workspace, go to the "Develop" tab, select the "MVAD_Visualization" notebook
- Run the provided Python code up to the Section 2, where you can uncomment to input the appropriate parameters, including the target scenario's associated ADX table.
- Run the following cell containing the `get_mvad_result_from_adx` function to get the preprocessed data and results from the target scenario's ADX table.
- Run the cell in Section 3, to call the `plot_mvad` function to get  the following plots, specifying the `min_severity` parameters, if you want to filter out possible false positives:
  - A plot of the selected time-series and the anomaly results. E.g.:

    ![plot_tsanom](../media/plot_tsanom.png)
  - A plot of the severity of the detected anomalies. E.g.:
  
    ![plot_severity](../media/plot_severity.png)
  - A plot of the contribution scores of the selected time-series for the detected anomalies. E.g.:
  - ![plot_contrib](../media/plot_contribscores.png)

In the example plots above, note that different detected anomalies can have different severity levels. Users are encouraged to additionally filter out anomalies that are not severe enough, using the `severity` result, to avoid too many false positives. See the [MVAD documentation](https://docs.microsoft.com/en-us/azure/cognitive-services/anomaly-detector/concepts/best-practices-multivariate) to learn more about severity and contribution scores.    

<br>

## Stopping inference pipeline runs
- Be mindful of the resource costs accumulating as the inference pipelines run in the background.
- To stop an inference run, 1. Click on Manage tab --> 2. Click on Trigger tab --> 3. Select the wanted trigger from the list (recall scenario name is the first part of the trigger name) --> 4. Select ‘Stopped’ as Status --> 5. Click on Ok --> 6. Click ‘Publish all’ to register the change made.
