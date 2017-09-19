# AzureServiceFabricBackupRestore
Source code implementation to backup and restore Azure Service Fabric Reliable Services and Actors

This Solution will help you take periodic backup of your Service Fabric Stateful Service and Actors and store it in Azure Blob Storage.  

In order to get started you should have the dev environment setup for Azure Service Fabric
Follow this link to setup Azure Service Fabric dev cluster on your local machine-
https://docs.microsoft.com/en-us/azure/service-fabric/service-fabric-get-started

You will also need to edit the credentials of Azure Blob Storage-
Open file /BackupActor.Interfaces/AzureBlobBackupManager.cs

string backupAccountName = "<your_azurestorage_accountname>";
string backupAccountKey = "<your_azurestorage_accountkey>";
