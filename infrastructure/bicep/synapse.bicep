param workspaceName string
param location string
param tags object
param adlsAccountName string
param adlsFileSystemName string
param sqlAdminLogin string
@secure()
param sqlAdminPassword string

// ── Synapse Workspace ────────────────────────────────────────────────────────
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: workspaceName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: 'https://${adlsAccountName}.dfs.core.windows.net'
      filesystem: adlsFileSystemName
    }
    sqlAdministratorLogin: sqlAdminLogin
    sqlAdministratorLoginPassword: sqlAdminPassword
    managedVirtualNetwork: 'default'
    publicNetworkAccess: 'Enabled'
  }
}

// ── Dedicated SQL Pool (DW100c for dev/cost savings) ────────────────────────
resource dedicatedSqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  parent: synapseWorkspace
  name: 'sqldw_fifa2026'
  location: location
  tags: tags
  sku: {
    name: 'DW100c'
  }
  properties: {
    createMode: 'Default'
    collation: 'SQL_Latin1_General_CP1_CI_AS'
  }
}

// ── Spark Pool ───────────────────────────────────────────────────────────────
resource sparkPool 'Microsoft.Synapse/workspaces/bigDataPools@2021-06-01' = {
  parent: synapseWorkspace
  name: 'sparkfifa2026'
  location: location
  tags: tags
  properties: {
    sparkVersion: '3.4'
    nodeCount: 3
    nodeSize: 'Medium'
    nodeSizeFamily: 'MemoryOptimized'
    autoScale: {
      enabled: true
      minNodeCount: 3
      maxNodeCount: 10
    }
    autoPause: {
      enabled: true
      delayInMinutes: 15
    }
    dynamicExecutorAllocation: {
      enabled: true
      minExecutors: 1
      maxExecutors: 4
    }
    sparkConfigProperties: {
      configurationType: 'Artifact'
    }
    // Delta Lake is built-in to Synapse Spark 3.x
    libraryRequirements: {
      content: 'scikit-learn==1.3.0\nmatplotlib==3.7.1\nseaborn==0.12.2\npandas==2.0.3\nnumpy==1.24.3\n'
      filename: 'requirements.txt'
    }
  }
}

// ── Firewall: Allow Azure Services ──────────────────────────────────────────
resource firewallAllowAzure 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  parent: synapseWorkspace
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

output workspaceName string = synapseWorkspace.name
output workspaceId string = synapseWorkspace.id
output dedicatedSqlPoolName string = dedicatedSqlPool.name
output sparkPoolName string = sparkPool.name
output synapseIdentityPrincipalId string = synapseWorkspace.identity.principalId
