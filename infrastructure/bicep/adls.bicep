@description('Storage account name (must be globally unique, lowercase, 3-24 chars)')
param storageAccountName string

param location string
param tags object

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true              // Hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    accessTier: 'Hot'
  }
}

// Blob service
resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

// Containers (filesystem layers)
var containers = ['raw', 'bronze', 'silver', 'gold', 'ml-models', 'synapse-artifacts']

resource fileSystem 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for name in containers: {
  parent: blobService
  name: name
  properties: {
    publicAccess: 'None'
  }
}]

output storageAccountName string = storageAccount.name
output storageAccountId string = storageAccount.id
output fileSystemName string = 'raw'    // primary filesystem for Synapse workspace
