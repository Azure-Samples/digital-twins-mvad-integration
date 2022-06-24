# 
#   Script to provision the resources of the anomaly detection pipeline and grant persmissions.
#   Please provide your existing Data History cluster and ADT instance information.
#
param(
    [Parameter(Mandatory)][String]$SubscriptionId,
    [Parameter(Mandatory)][String]$ResourceGroup,
    [Parameter(Mandatory)][String]$Location, 
    [Parameter(Mandatory)][String]$ADXResourceGroup, 
    [Parameter(Mandatory)][String]$ADXEndpoint,
    [Parameter(Mandatory)][String]$ADXClusterName,
    [Parameter(Mandatory)][String]$ADXDatabaseName,
    [Parameter(Mandatory)][String]$ADXTable,
    [Parameter(Mandatory)][String]$ADTEndpoint,
    [Parameter(Mandatory)][String]$ADTResourceGroup,
    [Parameter(Mandatory)][String]$SqlUser,
    # The password must be atleast 8 characters long and contain characters from three of the following four categories: (uppercase  letters, lowercase letters, digits (0-9),
    # Non-alphanumeric characters such as: !, $, #, or %).
    [Parameter(Mandatory)][String]$SqlPassword,
    # ADTEndpoint and ADXTable params not used for automated onboarding, but needed to set default Synapse pipeline parameters
    [String]$ADXSubscriptionId = $SubscriptionId,
    [String]$ADTSubscriptionId = $SubscriptionId,
    [String]$ADTInstanceName = $ADTEndpoint.Split(".")[0],
    [String]$SynapseWorkspaceName = "adt-synapse",
    [String]$SparkPoolNodeSize = "Small", 
    [String]$NotebookNodeCount = 1,
    [String]$KeyVaultName = "synapse-keyvault", 
    [String]$MVADResourceName = "adt-mvad",
    [String]$ADLSAccountName = "synapseadls",
    [String]$ADLSContainer = "user",
    [String]$AutoScaleMinNodeCount = 4,
    [String]$AutoScaleMaxNodeCount = 10,
    [String]$AutoPauseDelayInMinute = 5
)

# Check if the required packages exist
$ErrorActionPreference = 'Stop'
if (-not((Get-InstalledModule "Az") -and (Get-InstalledModule "Az.Synapse"))) {
    Write-Error "Please check if you have installed 'Az' and 'Az.Synapse' in your powershell."
}

# Import Az.Synapse module
Import-Module Az.Synapse

# Login AZ account, set subscription and register RP
Connect-AzAccount
Select-AzSubscription -SubscriptionId $SubscriptionId
if ((Register-AzResourceProvider -ProviderNamespace Microsoft.Synapse).RegistrationState -eq "Registering") {
    Write-Information "Registering Microsoft.Synapse..." -InformationAction Continue
    Start-Sleep -Seconds 20
}

# Set Qualifier to avoid existing names
$random = (-Join ((0x30..0x39) + ( 0x61..0x7A) | Get-Random -Count 8  | % {[char]$_}))
$KeyVaultName = $KeyVaultName + $random
$ADLSAccountName = $ADLSAccountName + $random
$MVADResourceName = $MVADResourceName + $random
$SynapseWorkspaceName = $SynapseWorkspaceName + $random

# Add https:// to ADT host name obtained from ADT instance in Azure portal
$ADTEndpoint = "https://" + $ADTEndpoint

# Check Resources Name Availability
function Test-AzNameAvailability {
    param(
        [Parameter(Mandatory = $true)] [string] $AuthorizationToken,
        [Parameter(Mandatory = $true)] [string] $SubscriptionId,
        [Parameter(Mandatory = $true)] [string] $Name,
        [Parameter(Mandatory = $true)] [ValidateSet(
            'KeyVault', 'Storage', 'Synapse')]
        $ServiceType
    )
 
    $uriByServiceType = @{
        KeyVault = 'https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.KeyVault/checkNameAvailability?api-version=2021-10-01'
        Storage  = 'https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Storage/checkNameAvailability?api-version=2021-04-01'
        Synapse  = 'https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Synapse/checkNameAvailability?api-version=2021-06-01'
    }
 
    $typeByServiceType = @{
        KeyVault = 'Microsoft.KeyVault/vaults'
        Storage  = 'Microsoft.Storage/storageAccounts'
        Synapse  = 'Microsoft.Synapse/workspaces'
    }
 
    $uri = $uriByServiceType[$ServiceType] -replace ([regex]::Escape('{subscriptionId}')), $SubscriptionId
    $body = '"name": "{0}", "type": "{1}"' -f $Name, $typeByServiceType[$ServiceType]
 
    $response = (Invoke-WebRequest -Uri $uri -Method Post -Body "{$body}" -ContentType "application/json" -Headers @{Authorization = $AuthorizationToken }).content
    $response | ConvertFrom-Json |
        Select-Object @{N = 'Name'; E = { $Name } }, @{N = 'Type'; E = { $ServiceType } }, @{N = 'Available'; E = { $_ | Select-Object -ExpandProperty *available } }, Reason, Message
}

function Get-AccesTokenFromCurrentUser {
    $azContext = Get-AzContext
    $azProfile = [Microsoft.Azure.Commands.Common.Authentication.Abstractions.AzureRmProfileProvider]::Instance.Profile
    $profileClient = New-Object -TypeName Microsoft.Azure.Commands.ResourceManager.Common.RMProfileClient -ArgumentList $azProfile
    $token = $profileClient.AcquireAccessToken($azContext.Subscription.TenantId)
    ('Bearer ' + $token.AccessToken)
}

$AuthorizationToken = Get-AccesTokenFromCurrentUser

if(!(Test-AzNameAvailability -Name $KeyVaultName -ServiceType KeyVault -AuthorizationToken $AuthorizationToken -SubscriptionId $SubscriptionId).Available) {
    Write-Error "KeyVault Name unavailable. The name should be globally unique and is a string of 3 to 24 characters that can contain only numbers (0-9), letters (a-z, A-Z), and hyphens (-)."
}

if(!(Test-AzNameAvailability -Name $ADLSAccountName -ServiceType Storage -AuthorizationToken $AuthorizationToken -SubscriptionId $SubscriptionId).Available) {
    Write-Error "Storage Account Name unavailable. The name might already exist or must be between 3 and 24 characters in length and may contain numbers and lowercase letters only."
}

if(!(Test-AzNameAvailability -Name $SynapseWorkspaceName -ServiceType Synapse -AuthorizationToken $AuthorizationToken -SubscriptionId $SubscriptionId).Available) {
    Write-Error "Synapse Workspace Name unavailable. The name might already exist or must be between 1 and 50 characters long, contain only lowercase letters or numbers or hyphens."
}

# Create a resource group
Write-Information -MessageData "Creating Resource Group..." -InformationAction Continue
New-AzResourceGroup -Name $ResourceGroup -Location $Location
Write-Information -MessageData "Created Resource Group $ResourceGroup" -InformationAction Continue

# Create MVAD Instance and Set Connection Key
Write-Information -MessageData "Creating MVAD Resources..." -InformationAction Continue
New-AzCognitiveServicesAccount -ResourceGroupName $ResourceGroup -name $MVADResourceName -Type AnomalyDetector -SkuName S0 -Location $Location -ErrorAction Stop
while (!((Get-AzCognitiveServicesAccount -ResourceGroupName $ResourceGroup -name $MVADResourceName).ProvisioningState -eq "Succeeded"))
{
    Start-Sleep -Seconds 5
}
New-AzCognitiveServicesAccountKey -ResourceGroupName $ResourceGroup -name $MVADResourceName -keyname Key1 | Out-Null
$MVADKey = (Get-AzCognitiveServicesAccountKey -ResourceGroupName $ResourceGroup -name $MVADResourceName).key1
$MVADKey = ConvertTo-SecureString $MVADKey -AsPlainText -Force
Write-Information -MessageData "MVAD Account Created" -InformationAction Continue

# Create ADLS Storage Account and Set ConnectionString
Write-Information -MessageData "Creating ADLS Account..." -InformationAction Continue
New-AzStorageAccount -ResourceGroupName $ResourceGroup -AccountName $ADLSAccountName -Location $Location -SkuName Standard_GRS -Kind StorageV2 -EnableHierarchicalNamespace $true
New-AzStorageAccountKey -ResourceGroupName $ResourceGroup -Name $ADLSAccountName -KeyName key1
$ADLSAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $ResourceGroup -AccountName $ADLSAccountName)[0].value
$ctx = New-AzStorageContext -StorageAccountName $ADLSAccountName -StorageAccountKey $ADLSAccountKey
Start-Sleep -Seconds 15
New-AzStorageContainer -Context $ctx -Name $ADLSContainer
$ADLSConnectionString = 'DefaultEndpointsProtocol=https;AccountName=' + $ADLSAccountName + ';AccountKey=' + $ADLSAccountKey + ';EndpointSuffix=core.windows.net' 
$ADLSConnectionString = ConvertTo-SecureString $ADLSConnectionString -AsPlainText -Force
Write-Information -MessageData "ADLS Account Created" -InformationAction Continue

# Create KeyVault and Store Connection Secrets
Write-Information -MessageData "Creating KeyVault..." -InformationAction Continue
New-AzKeyVault -Name $KeyVaultName -ResourceGroupName $ResourceGroup -Location $Location
Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name ad-poc -SecretValue $MVADKey
Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name adls-connection-string -SecretValue $ADLSConnectionString
# Update kv linkedService file 
$kvjson = Get-Content 'linkedService/KeyVault.json' -raw | ConvertFrom-Json
$kvjson.properties.typeProperties.baseUrl = "https://" + $KeyVaultName + ".vault.azure.net/"
$kvjson | ConvertTo-Json -depth 32| set-content 'linkedService/KeyVault.json'
Write-Information -MessageData "KeyVault created" -InformationAction Continue

# Update Kusto linkedService file
$kustojson = Get-Content 'linkedService/DataExplorer.json' -raw | ConvertFrom-Json
$kustojson.properties.typeProperties.endpoint = $ADXEndpoint
$kustojson.properties.typeProperties.database = $ADXDatabaseName
$kustojson | ConvertTo-Json -depth 32| set-content 'linkedService/DataExplorer.json'

# Create Synapse WorkSpace
Write-Information -MessageData "Creating Synapse Resources..." -InformationAction Continue
$Cred = New-Object -TypeName System.Management.Automation.PSCredential ($SqlUser, (ConvertTo-SecureString $SqlPassword -AsPlainText -Force))
$WorkspaceParams = @{
  Name = $SynapseWorkspaceName
  ResourceGroupName = $ResourceGroup
  DefaultDataLakeStorageAccountName = $ADLSAccountName
  DefaultDataLakeStorageFilesystem = $ADLSContainer
  SqlAdministratorLoginCredential = $Cred
  Location = $Location
}
New-AzSynapseWorkspace @WorkspaceParams

$WorkspaceWeb = (Get-AzSynapseWorkspace -Name $SynapseWorkspaceName -ResourceGroupName $ResourceGroup).ConnectivityEndpoints.web
$WorkspaceDev = (Get-AzSynapseWorkspace -Name $SynapseWorkspaceName -ResourceGroupName $ResourceGroup).ConnectivityEndpoints.dev

$FirewallParams = @{
    WorkspaceName = $SynapseWorkspaceName
    Name = 'Allow Client IP'
    ResourceGroupName = $ResourceGroup
    StartIpAddress = "0.0.0.0"
    EndIpAddress = "255.255.255.255"
  }
New-AzSynapseFirewallRule @FirewallParams

# Populate Synapse pipeline parameters with default values
$trainingpipelinejson = Get-Content 'pipeline/consolidated_training_pipeline.json' -raw | ConvertFrom-Json
$trainingpipelinejson.properties.variables.adls_account.defaultValue = $ADLSAccountName
$trainingpipelinejson.properties.variables.adls_container.defaultValue = $ADLSContainer
$trainingpipelinejson.properties.variables.kusto_database.defaultValue = $ADXDatabaseName
$trainingpipelinejson.properties.variables.kv_name.defaultValue = $KeyVaultName
$trainingpipelinejson.properties.variables.adt_endpoint.defaultValue = $ADTEndpoint
$trainingpipelinejson.properties.parameters.adx_table.defaultValue = $ADXTable
$trainingpipelinejson.properties.variables.mvad_region.defaultValue = $Location
$trainingpipelinejson.properties.variables.synapse_endpoint.defaultValue = $WorkspaceDev
$trainingpipelinejson | ConvertTo-Json -depth 32| set-content 'pipeline/consolidated_training_pipeline.json'

$PrincipalId = (Get-AzSynapseWorkspace -ResourceGroupName $ResourceGroup -Name $SynapseWorkspaceName).Identity.PrincipalId
$TenantId =  (Get-AzSynapseWorkspace -ResourceGroupName $ResourceGroup -Name $SynapseWorkspaceName).Identity.TenantId

New-AzSynapseSparkPool -WorkspaceName $SynapseWorkspaceName -Name newSpark3 -AutoScaleMinNodeCount $AutoScaleMinNodeCount -AutoScaleMaxNodeCount $AutoScaleMaxNodeCount -SparkVersion 3.1 -NodeSize $SparkPoolNodeSize -EnableAutoPause -AutoPauseDelayInMinute $AutoPauseDelayInMinute
Set-AzSynapseNotebook -WorkspaceName $SynapseWorkspaceName -Name consolidated_training -DefinitionFile "notebook/consolidated_training.ipynb"  -SparkPoolName newSpark3 -ExecutorCount $NotebookNodeCount
Set-AzSynapseNotebook -WorkspaceName $SynapseWorkspaceName -Name consolidated_inference -DefinitionFile "notebook/consolidated_inference.ipynb"  -SparkPoolName newSpark3 -ExecutorCount $NotebookNodeCount
Set-AzSynapseNotebook -WorkspaceName $SynapseWorkspaceName -Name MVAD_Visualization -DefinitionFile "notebook/MVAD_Visualization.ipynb"  -SparkPoolName newSpark3 -ExecutorCount $NotebookNodeCount
Set-AzSynapseLinkedService -WorkspaceName $SynapseWorkspaceName -Name ADT_AnomalyDetector_KeyVault -DefinitionFile "linkedService/KeyVault.json"
Set-AzSynapseLinkedService -WorkspaceName $SynapseWorkspaceName -Name ADT_Data_History -DefinitionFile "linkedService/DataExplorer.json"
Set-AzSynapsePipeline -WorkspaceName $SynapseWorkspaceName -Name consolidated_training_pipeline -DefinitionFile "pipeline/consolidated_training_pipeline.json"
Set-AzSynapsePipeline -WorkspaceName $SynapseWorkspaceName -Name consolidated_inference_pipeline -DefinitionFile "pipeline/consolidated_inference_pipeline.json"
Write-Information -MessageData "Synapse resources created" -InformationAction Continue

# Grant Permissions to the Synapse Workspace
Write-Information -MessageData "Granting Access to your Synapse workspace..." -InformationAction Continue
Set-AzKeyVaultAccessPolicy -VaultName $KeyVaultName -ObjectId $PrincipalId -PermissionsToSecrets get
New-AzKustoDatabasePrincipalAssignment -ResourceGroupName $ADXResourceGroup -ClusterName $ADXClusterName -DatabaseName $ADXDatabaseName -SubscriptionId $ADXSubscriptionId -PrincipalAssignmentName $SynapseWorkspaceName -PrincipalId $PrincipalId -TenantId $TenantId -PrincipalType App -Role Admin
$Params = @{
  ObjectId = $PrincipalId
  RoleDefinitionName = 'Azure Digital Twins Data Reader'
  Scope = '/subscriptions/' + $ADTSubscriptionId + '/resourceGroups/'+ $ADTResourceGroup + '/providers/Microsoft.DigitalTwins/digitalTwinsInstances/' + $ADTInstanceName
}
New-AzRoleAssignment @Params
Write-Information -MessageData "Permissions Granted" -InformationAction Continue

Write-Information -MessageData "Resources Creation Succeeded. Access Synapse workspace at $WorkspaceWeb." -InformationAction Continue