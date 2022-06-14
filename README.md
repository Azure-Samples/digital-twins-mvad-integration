# Azure Digital Twins Multivariate Anomaly Detection Toolkit

Azure Digital Twins Multivariate Anomaly Detection Toolkit is a sample project to showcase a low-code integration of [Azure Digital Twins](https://docs.microsoft.com/en-us/azure/digital-twins/overview) with [Azure AI and Cognitive Services' Anomaly Detector](https://azure.microsoft.com/en-us/services/cognitive-services/anomaly-detector/), an advanced AI solution to help identify abnormal operations or defective assets in near real-time.

This integration toolkit leverages Synapse Analytics as the UI platform to host and run the toolkit code. ADT users can thus easily onboard and quickly connect to a downstream AI/ML solution. The goal of this toolkit is to allow ADT users to easily leverage the power of the twin graph, with historized time-series data, to identify anomalies in modelled environments. This is done by:
- Defining one or more scenarios:
  - [Querying the twin graph](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-query-graph) to provide a scenario's wanted **context** by leveraging ADT's ADX plugin,
  - Selecting the target properties in the scenario, i.e. the multiple variables, to later **analyze all together in a multivariate** fashion,
  - Gathering the selected variables' **historized time-series data** through [ADT's Data History feature](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-use-data-history?tabs=cli) or providing the associated ADX table,
- For each scenario, running the **training** pipeline so that the AI model learns the scenario's system's **normal behavior** from the historized data. 
- Running the **inference** pipeline so that the scenario's associated trained AI model can detect **anomalous behavior** in near real-time.
- Accessing and visualizing the anomaly detection results in an ADX table.
![synapse workflow](./media/Synapse-workflow.png)

## Features
This project provides the following features:

* ADT-MVAD toolkit (See [./MVADToolkit/README.md](./MVADToolkit/README.md/#about) for more info):
  - Automatic onboarding: automatic provisioning and linking of the necessary Azure resources done through a Powershell script (`./MVADToolkit/setup.ps`). 
  - Synapse artifacts 
    - Configurations are provided for Synapse workflow: the Training and inference pipelines to define scenarios through user-input, help orchestrate and pass meta-data between training and inference, and surface any errors.
    - Code in Synapse notebooks that:
      - Preprocess the raw data and format data to be taken in readily by MVAD, and optionally smoothen training data
      - Call MVAD apis to run training or inference
      - Visualization ploats to interpret raw anomaly results
    - The configuration and code can easily modified and extended by user to suit their needs


* Sample dataset (See [./Synthetic Data Generation/README.md](./MVADToolkit/README.md/#about) for more info):
  - To help you test-drive this toolkit easily, we provide code to prop up sample ADT twins, and synthetically generate sample time-series data.
  - We provide code to pump the sample time-series data as twins' property updates. Alternatively, the time-series can be uploaded to the linked ADX table.


## How to use this sample

### Prerequisites

* To analyze your assets with MVAD, ensure you have an Azure Digital Twins instance that encompasses the target system, ie. cluster of assets ([see the ADT docs for how-to](https://docs.microsoft.com/en-us/azure/digital-twins/overview)) . If you do not have one, set up a sample one according to the [section below](#optional-sample-adt-instance-and-historized-data).
* Ensure you have Owner and Azure Digital Twins Data Reader role in your Azure Digital Twins instance through Azure Portal
* Ensure you have Owner role in the associated ADX through Azure Portal
* To run the setup script, install the following modules in Powershell, if you don't already have it installed:
  * Azure Powershell: [Documentation](https://docs.microsoft.com/en-us/powershell/azure/install-az-ps?view=azps-7.3.2)
  * Azure Synapse Powershell module: [Documentation](https://docs.microsoft.com/en-us/azure/synapse-analytics/quickstart-create-workspace-powershell#install-the-azure-synapse-powershell-module)

### Installation

- Clone this repo
- Provision the required resources (See [./MVADToolkit/README.md](./MVADToolkit/README.md/#about) for more info):

  - Determine the resources parameters for the Powershell setup script and run the script.
  - Run the PowerShell setup script. This will take 5-10 mins.
  ![synapse resources](./media/Synapse-resources.png)
- If you are bringing in your own ADT instance, ensure there is an appropriate time-series data source for your MVAD analysis in the ADX table. If you do not have one, set up a sample one according to the [section below](#optional-sample-adt-instance-and-historized-data).

### Quickstart



- Scenario definition
- training pipeline run
- inferencing pipeline run
- Accessing & visualizing [optional] results
-- [Optional] Sample ADT instance and historized data
Test

See the [toolkit's user-guide](./MVADToolkit/README.md/#user-guide), for more details on how to run a scenario end-to-end.

## Toolkit Demo

#TODO: Add demo video


## Resources


- [ADT resources](https://docs.microsoft.com/en-us/azure/digital-twins/overview) 
  - [ADT query](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-query-graph)
  - [ADT ADX plugin](https://docs.microsoft.com/en-us/azure/digital-twins/concepts-data-explorer-plugin)
  - [ADT Data History](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-use-data-history?tabs=cli)
- Cognitive services' Anomaly Detector resources(https://docs.microsoft.com/en-us/azure/cognitive-services/anomaly-detector/overview-multivariate) 
