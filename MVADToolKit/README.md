# About
This folder is to help you get started with your ADT-MVAD project. It contains a set of pipeline artifacts and a powershell script that will help provision necessary resources and link them together to Synapse workspace.

# Structure
```linkedService/*```: Templates of linkedServices to your Synapse workspace. Values will be automatically populated by the set up script.   
```notebook/*```: Notebooks of ADT-MVAD data processing and training/inference modules.   
```pipeline/*```: Synapse training/inference Pipeline templates.   
```setup.ps1```: Powershell script to provision resources, link resources to Synapse workspace, and grant necessary permissions. 

# Get Started
To run ```setup.ps1```, provide these parameters:
#### General
- ```SubscriptionId``` [required]: The subscription where all the newly provisioned resources reside in.
- ```ResourceGroup``` [required]: The script will create new resource group.
- ```Location``` [required]: Region of the resource group.
#### ADX related
- ```ADXSubscriptionID``` [optional]: The subscription ID of your Data Hisotry ADX cluster. Default to the same subscription for all your resources (SubscriptionId in General section above).
- ```ADXResourceGroup``` [required]: ADX cluster resource group. 
- ```ADXEndpoint``` [required]: ADX URI (obtained from ADX instance in Azure portal).
- ```ADXClusterName``` [required]: ADX cluster name.
- ```ADXDatabaseName``` [required]: ADX cluster database name.  
- ```ADXTable``` [required]: ADX cluster table name used for ADT data history.

#### ADT related
- ```ADTSubscriptionId``` [optional]: The subscription ID of your ADT Instance. Default to the same subscription for all your resources (SubscriptionId in General section above).
- ```ADTResourceGroup``` [required]: ADT resource group.
- ```ADTEndpoint``` [required]: ADT host name (obtained from ADT instance in Azure portal).

#### Synapse Sql User
- ```SqlUser``` [required]: Synapse workspace Sql user.
- ```SqlPassword``` [required]: Synapse workspace Sql password. The password must be atleast 8 characters long and contain characters from three of the following four categories: (uppercase  letters, lowercase letters, digits (0-9), Non-alphanumeric characters such as: !, $, #, or %).

#### Default Resource Names [Optional]
- ```SynapseWorkspaceName```: Synapse workspace name, defaults to ```adt-synapse```.
- ```KeyVaultName```: Key Vault name, defaults to ```synapse-keyvault```.
- ```MVADResourceName```: MVAD name, defaults to ```adt-mvad```.
- ```ADLSAccountName```: ADLS name, defaults to ```synapseadls```.
- ```ADLSContainer```: ADLS container name, defaults to ```user```.

#### Default Resource Configuration [Optional]
- ```SparkPoolNodeSize```: Synapse spark pool Node Size, defaults to ```Small``` (4 vCores / 32 GB memory).
- ```AutoScaleMinNodeCount```: Minimum Synapse spark pool Node Count, defaults to ```4```.
- ```AutoScaleMaxNodeCount```: Maximum Synapse spark pool Node Count, defaults to ```10```.
- ```AutoPauseDelayInMinute```: Synapse spark pool autopausing policy, defaults to ```5```.
- ```NotebookNodeCount```: Synapse notebook Spark pool worker node allocation, defaults to ```1```.

# Run
A sample command will look like this:  
```.\setup.ps1 -SubscriptionId <subscriptionId> -ResourceGroup <rg> -Location <location> -ADXResourceGroup <adx_rg> -ADXEndpoint <adx_endpoint> -ADXClusterName <cluster> -ADXDatabaseName <database> -ADXTable <table> -ADTResourceGroup <adt_rg> -ADTEndpoint <adt_endpoint> -SqlUser <user> -SqlPassword <password>```

The script will run. Log to your Azure account via the pop up window and check the logs for progress. After script finishes, a url to your synapse workspace will be returned.