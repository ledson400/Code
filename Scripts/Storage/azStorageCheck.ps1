# Requires PowerShell 7+
# Prerequisites

param (
    [string]$storageAccountName,
    [string]$resourceGroup
)

# Check if both parameters are provided
if (-not $storageAccountName -or -not $resourceGroup) {
    Write-Host "Error: Both -storageAccountName and -resourceGroup parameters are required." -ForegroundColor Red
    Write-Host "Usage: .\azStorageCheck.ps1 -storageAccountName <SAname> -resourceGroup <RG>" -ForegroundColor Yellow
    exit 1
}

Import-Module Az
Import-Module AzTable

# Configuration
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = "table_storage_usage_$timestamp.csv"

# Connect to Azure and get context
# Connect-AzAccount   # Uncomment if needed
$storageAccount = Get-AzStorageAccount -ResourceGroupName $resourceGroup -Name $storageAccountName
$ctx = $storageAccount.Context
$keys = Get-AzStorageAccountKey -ResourceGroupName $resourceGroup -Name $storageAccountName
$key = $keys[0].Value

# Get all table references
$tables = Get-AzStorageTable -Context $ctx

# Build parameter list
$tableParams = foreach ($table in $tables) {

    [PSCustomObject]@{
        TableName    = $table.CloudTable.Name
        AccountName  = $storageAccountName
        AccountKey   = $key
        StorageUri   = $ctx.TableEndpoint.AbsoluteUri
    }
}

# Create a named mutex for synchronized file access
$mutexName = "AzStorageCheckMutex"
$mutex = New-Object System.Threading.Mutex($false, $mutexName)

# Run parallel processing
$results = $tableParams | ForEach-Object -Parallel {
    $tableName   = $_.TableName
    $accountName = $_.AccountName
    $accountKey  = $_.AccountKey
    $storageUri  = $_.StorageUri

    if (-not $accountName) {
        Write-Warning "Skipping $tableName due to missing accountName"
        return
    }

    try {
        Write-Host "Processing table: $tableName"

        # Build context inside the parallel thread
        $ctx = New-AzStorageContext -StorageAccountName $accountName -StorageAccountKey $accountKey
        $tableRef = (Get-AzStorageTable -Name $tableName -Context $ctx).CloudTable

        # Query table
        $query = [Microsoft.Azure.Cosmos.Table.TableQuery]::new()
        $sample = $tableRef.ExecuteQuery($query) | Select-Object -First 20
        $sampleCount = $sample.Count

        if ($sampleCount -eq 0) {
            $avgSize = 0
            $totalEntities = 0
        } else {
            $totalSize = 0
            foreach ($entity in $sample) {
                $json = $entity | ConvertTo-Json -Depth 5
                $totalSize += $json.Length
            }

            $avgSize = [math]::Round($totalSize / $sampleCount, 2)

            # Count all entities
            $totalEntities = 0
            $token = $null
            do {
                $segment = $tableRef.ExecuteQuerySegmented($query, $token)
                $totalEntities += $segment.Results.Count
                $token = $segment.ContinuationToken
            } while ($token -ne $null)
        }

        $estKB = [math]::Round(($avgSize * $totalEntities) / 1024, 2)
        $estMB = [math]::Round($estKB / 1024, 2)

        $result = [PSCustomObject]@{
            TableName       = $tableName
            EntityCount     = $totalEntities
            AvgRowSizeBytes = $avgSize
            EstimatedSizeKB = $estKB
            EstimatedSizeMB = $estMB
        }

        # Append to log file safely using mutex
        $mutex = [System.Threading.Mutex]::OpenExisting($using:mutexName)
        $mutex.WaitOne() | Out-Null
        try {
            if ($result -ne $null) {
                $result | Export-Csv -Path $using:logFile -NoTypeInformation -Append
            }
        } finally {
            $mutex.ReleaseMutex() | Out-Null
            $mutex.Close()
        }

        $result

    } catch {
        Write-Warning "Error processing table ${tableName}: $_"
        return $null
    }
} -ThrottleLimit 4

# Clean up mutex
$mutex.Close()

# Clean and display results
$results = $results | Where-Object { $_ -ne $null }
$results | Sort-Object EstimatedSizeMB -Descending | Format-Table -AutoSize

Write-Host "`nLog written to: $logFile"
