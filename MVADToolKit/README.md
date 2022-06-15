# About

This folder contains the ADT-MVAD toolkit, i.e.  a set of pipeline artifacts and a powershell script that will help provision necessary resources and link them together to Synapse workspace.

# Toolkit Structure

`linkedService/*`: Templates of linkedServices to your Synapse workspace. Values will be automatically populated by the set up script.
`notebook/*`: Notebooks of ADT-MVAD data processing for training/inference modules and visualization of anomaly results.
`pipeline/*`: Synapse training/inference pipeline templates.
`setup.ps1`: Powershell script to provision resources, link resources to Synapse workspace, and grant necessary permissions.

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

## Run setup script

A sample command will look like this:

`.\MVADToolkit\setup.ps1 -SubscriptionId <subscriptionId> -ResourceGroup <rg> -Location <location> -ADXResourceGroup <adx_rg> -ADXEndpoint <adx_endpoint> -ADXClusterName <cluster> -ADXDatabaseName <database> -ADXTable <table> -ADTResourceGroup <adt_rg> -ADTEndpoint <adt_endpoint> -SqlUser <user> -SqlPassword <password> `

- This will run for 5-10 mins.
- If there are permission errors, use the following command:` Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`
- While the script runs, you can log to your Azure account and check the logs for progress. Once the script has finished running, a url to your **Synapse workspace** will be returned.


# Dependencies
This toolkit currently implements MVAD version 1.1, as set in the SynapseML version: `com.microsoft.azure:synapseml_2.12:0.9.5-19-82d6b563-SNAPSHOT`, that runs in the Synapse pools.

# User-guide 

  ![Training-inf-dataflow](../media/Train-Inf-Data-flow.png)
## Scenario definition

## training pipeline run

## inferencing pipeline run

## Accessing & visualizing [optional] results

# Troubleshooting
