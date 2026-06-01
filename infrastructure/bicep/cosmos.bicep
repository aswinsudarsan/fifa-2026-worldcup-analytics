param accountName string
param location string
param tags object

// ── Cosmos DB Account with Synapse Link enabled ──────────────────────────────
resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2023-04-15' = {
  name: accountName
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    enableAnalyticalStorage: true       // enables Synapse Link / analytical store
    analyticalStorageConfiguration: {
      schemaType: 'WellDefined'
    }
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    capabilities: [
      { name: 'EnableServerless' }      // serverless for dev cost savings
    ]
  }
}

// ── Database ─────────────────────────────────────────────────────────────────
resource cosmosDatabase 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-04-15' = {
  parent: cosmosAccount
  name: 'fifa2026'
  properties: {
    resource: {
      id: 'fifa2026'
    }
  }
}

// ── Containers with analytical store enabled ─────────────────────────────────
var containers = [
  { name: 'live_matches', partitionKey: '/match_id' }
  { name: 'live_scores', partitionKey: '/match_id' }
  { name: 'player_events', partitionKey: '/player_id' }
]

resource cosmosContainers 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-04-15' = [for c in containers: {
  parent: cosmosDatabase
  name: c.name
  properties: {
    resource: {
      id: c.name
      partitionKey: {
        paths: [c.partitionKey]
        kind: 'Hash'
      }
      analyticalStorageTtl: -1         // infinite TTL in analytical store
    }
  }
}]

output accountName string = cosmosAccount.name
output accountId string = cosmosAccount.id
output databaseName string = cosmosDatabase.name
