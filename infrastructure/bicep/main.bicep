@description('Project prefix for all resource names')
param projectPrefix string = 'fifa2026'

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Environment tag')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('SQL Pool admin login')
param sqlAdminLogin string = 'sqladmin'

@secure()
@description('SQL Pool admin password')
param sqlAdminPassword string

var tags = {
  project: 'FIFA2026WorldCup'
  environment: environment
  owner: 'DataEngineering'
}

// ── ADLS Gen2 ────────────────────────────────────────────────────────────────
module adls 'adls.bicep' = {
  name: 'adlsDeploy'
  params: {
    storageAccountName: 'adls${projectPrefix}${environment}'
    location: location
    tags: tags
  }
}

// ── Synapse Workspace ────────────────────────────────────────────────────────
module synapse 'synapse.bicep' = {
  name: 'synapseDeploy'
  params: {
    workspaceName: 'synapse-${projectPrefix}-${environment}'
    location: location
    tags: tags
    adlsAccountName: adls.outputs.storageAccountName
    adlsFileSystemName: adls.outputs.fileSystemName
    sqlAdminLogin: sqlAdminLogin
    sqlAdminPassword: sqlAdminPassword
  }
}

// ── Azure Data Factory ───────────────────────────────────────────────────────
resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: 'adf-${projectPrefix}-${environment}'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    globalParameters: {
      environment: {
        type: 'String'
        value: environment
      }
      adlsAccountName: {
        type: 'String'
        value: adls.outputs.storageAccountName
      }
    }
  }
}

// ── Cosmos DB (for Synapse Link) ─────────────────────────────────────────────
module cosmos 'cosmos.bicep' = {
  name: 'cosmosDeploy'
  params: {
    accountName: 'cosmos-${projectPrefix}-${environment}'
    location: location
    tags: tags
  }
}

// ── Key Vault ────────────────────────────────────────────────────────────────
resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: 'kv-${projectPrefix}-${environment}'
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: tenant().tenantId
    enableRbacAuthorization: true
    softDeleteRetentionInDays: 7
  }
}

// ── Outputs ──────────────────────────────────────────────────────────────────
output synapseWorkspaceName string = synapse.outputs.workspaceName
output adfName string = adf.name
output adlsAccountName string = adls.outputs.storageAccountName
output cosmosAccountName string = cosmos.outputs.accountName
output keyVaultName string = keyVault.name
