function ConnectToAzPowershell {
    try {
        $loggedinSubscription=Get-AzSubscription -ErrorAction Stop
        if ($loggedinSubscription){
            Write-Output "Already Logged in"
            Get-AzContext
            return $true
        }
    } catch {
        try {
            $thumb = "FF3D8BCB5EDC72AFEBB1C4E362B61D209BF0D6B1"
            #$appid = Get-AzADApplication | where DisplayName -match "unattendedlogin"
            #$appid.AppId
            #$tenantid = Get-AzureADTenantDetail
            #$tenantid.ObjectId
            Write-OutWithDateTime "Not Logged In.  Trying."
            Connect-AzAccount -TenantId $tenantid -ApplicationId $appid -CertificateThumbprint $thumb -Subscription $subscriptionid -ErrorAction Stop
            #This script requires system identity enabled for the automation account with 'Automation Contributor' role assignment on the identity.
            #Connect-AzAccount -Identity
        } catch {
            Write-OutWithDateTime "Failed To Login." 
            #Write-Error -Message $_.Exception
            throw 
        }
    }
}
function ConnectToAzureCLI{
    az login --service-principal --username $appid --tenant $tenantid --password /NexpointAzureVHDBackups/combined.pem
}
function EnableAndGetDownloadURL([String]$RG,[String]$DN){
    Write-OutWithDateTime "EnableAndGetDownloadURL_Function"
    $downloadURL = $null
    try {
    $downloadURL = Grant-AzDiskAccess -ResourceGroupName $RG -DiskName $DN -DurationInSecond 86400 -Access 'Read' -ErrorAction Stop -ErrorVariable e
    return $downloadURL.AccessSAS  
    } catch {
        Write-OutWithDateTime "WARNING::::Failed to Enable and Download URL "
        Write-Error -Message e
        return $null
    }
}
function DownloadTheDiskAsVHD([String]$BlobURL,[String]$DestinationPath){
    Write-OutWithDateTime "DownloadTheDiskAsVHD_Function"
    try{
        $blob = Get-AzStorageBlobContent -Uri $BlobURL -Destination $DestinationPath -Force -ErrorAction Stop -ErrorVariable e
        #Write-Host $blob.name $blob.length
        return @($blob.name,$blob.length)
    } catch{
        Write-OutWithDateTime (-join("WARNING::::Failed To Download Disk ", $blob))
        Write-Error -Message e
        return $null
    }   
}
function RenameDownloadedVHD([String]$OriginalFilePath,[String]$OriginalFilename,[String]$NewFileName){
    [string]$originalfileAndPath=(-join($OriginalFilePath,"\",$OriginalFilename)) 
    [string]$renamedFileName=(-join($NewFileName,".vhd"))
    Write-OutWithDateTime (-join("Renaming ",$originalfileAndPath," ",$renamedFileName))
    try {
        Rename-Item -Path $originalfileAndPath -NewName $renamedFileName -ErrorAction Stop
        return $true 
    } catch {
        Write-OutWithDateTime (-join("WARNING::::File Rename Failed ",$OriginalFilePath," ",$OriginalFilename," ",$NewFileName))
        return $null 
    }
}function GetRecoveryPointTime([String]$RecoveryPointName,[String]$ContainerName,[String]$ItemName){
    Write-OutWithDateTime "GetRecoveryPointTime_Function"
    az backup recoverypoint show --container-name $ContainerName --resource-group nxpBackup --vault-name NXPBackupVault --backup-management-type AzureIaasVM `
    --item-name $ItemName --name $RecoveryPointName --query properties.recoveryPointTime
}    
function GetLatestRecoveryPoint([String]$ContainerName,[String]$ItemName){
    Write-OutWithDateTime "GetLatestRecoveryPoint_Function"
    WriteDateTimeToOutfile "--------GetLatestRecoveryPoint_Function--------" | Out-Null
    $null=az backup recoverypoint list --resource-group nxpBackup --vault-name NXPBackupVault --backup-management-type AzureIaasVM --container-name $ContainerName `
     --item-name $ItemName | Out-File -FilePath "C:\NexpointAzureVHDBackups\$rundateTime command_output.txt" -append
    return az backup recoverypoint list --resource-group nxpBackup --vault-name NXPBackupVault --backup-management-type AzureIaasVM --container-name $ContainerName `
     --item-name $ItemName --query [0].name
}
function GetCleanRecoveryPointDateTime([String]$RawDateTime){
    $(ExtractDate $RawDateTime)+ "_"+ $(ExtractTime $RawDateTime)
}
function ExtractDate([String]$theDateTime){
    $theDateTime.Substring(1,4) + $theDateTime.Substring(6,2) + $theDateTime.Substring(9,2)
}
function ExtractTime([String]$theDateTime){
    $theDateTime.Substring(12,2) + $theDateTime.Substring(15,2) + $theDateTime.Substring(18,2)
}
function GetJustDiskName([String]$rawDiskName){
    $tslen=$rawDiskName.Length
    $rawDiskName.Substring(0,($tslen-16))
}
function GetBackupContainerLongName([String]$ContainerName){
    Write-OutWithDateTime "GetBackupContainerLongName_Function"
    if ($ContainerName){
    $tmpObject=az backup container list --backup-management-type AzureIaasVM --resource-group nxpBackup --vault-name NXPBackupVault | ConvertFrom-Json
    $tmpObject=($tmpObject | where-Object {$_.id -like "*$ContainerName*"} | Select-Object name).Name
   }
   if (!$tmpObject) {
    Write-OutWithDateTime 
    throw "Unable to get Container Long File Name"
   } else {
    return $tmpObject
   }
}
function RestoreTheDisks([String]$LongContainerName,[string]$ItemName,[string]$RestorePointName){
    Write-OutWithDateTime "RestoreTheDisks_Function"
    WriteDateTimeToOutfile "--------RestoreTheDisks_Function--------"  | Out-Null
    $tout=az backup restore restore-disks --resource-group nxpBackup --vault-name NXPBackupVault --container-name $LongContainerName --item-name $ItemName `
    --rp-name $RestorePointName --target-resource-group nxpRestoredVMDisks --storage-account nxpunattendedrestore --storage-account-resource-group 'nxpRestoredVMDisks'
    $tout | Out-File -FilePath "C:\NexpointAzureVHDBackups\$rundateTime command_output.txt" -append
    return $tout
}
function GetStatusOfRestoreJob([String]$RestoreGUID) {
    $tmpObject=az backup job list --resource-group nxpBackup --vault-name NXPBackupVault --query "[?name=='$RestoreGUID']" | ConvertFrom-Json
    $tmpObject.properties.status
}
function GetRestoreJobGUID([String]$RestoreJobObject){
    $tmpObject=$RestoreJobObject | ConvertFrom-Json
    $tmpObject.Name
}
function RemoveDiskFromRG([String]$DiskName){
    Write-OutWithDateTime (-join("Revoke Access for disk ",$DiskName))
    Revoke-AzDiskAccess -ResourceGroupName "nxpRestoredVMDisks" -DiskName $DiskName -ErrorAction SilentlyContinue
    try {
        Remove-AzDisk -ResourceGroupName "nxpRestoredVMDisks" -DiskName $DiskName -Force -ErrorAction Stop
        Write-OutWithDateTime (-join("Successfully Removed Disk ", $DiskName," From RG"))
        return $true
    } catch {
        Write-OutWithDateTime -Message (-join("WARNING::::Could Not Remove Disk ", $DiskName))
        return $false
    }
}
function Write-OutWithDateTime([String]$Message){
    $tdateTime=get-date -format "yyyy/MM/dd HH:mm:ss"
    Write-Information -MessageData (-join($tdateTime," ",$Message)) -InformationAction Continue
}
function WriteDateTimeToOutfile([String]$Text){
    $tdateTime=get-date -format "yyyy/MM/dd HH:mm:ss"
    Write-Output (-join($tdateTime," ",$Text | Out-File -FilePath "C:\NexpointAzureVHDBackups\$rundateTime command_output.txt" -Append))
}
function GetListOfDisks{
    try{
        $tmp=get-azResource -ResourceGroupName $disksResourceGroup -ResourceType "Microsoft.Compute/disks" -ErrorAction Stop -ErrorVariable evar
        WriteDateTimeToOutfile "--------GetListOfDisks--------"  | Out-Null
        $tmp | Out-File -FilePath "C:\NexpointAzureVHDBackups\$rundateTime command_output.txt" -Append 
        return $tmp
    } catch {
        Write-OutWithDateTime "WARNING::::Get List Of Disks Failed. Can't Continue"
        throw 
    }
}
function AttachToSynology{
    #$sUsername="azuredatasync"
    #$PWord = ConvertTo-SecureString -String $synologyPassword -AsPlainText -Force
    #$Credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $sUsername, $PWord
    #New-PSDrive -Name U -PSProvider FileSystem -Root '\\192.168.222.20\Azure_Backups' -Credential $Credential -ErrorAction Stop
    Remove-SmbMapping -LocalPath "U:" -Force -ErrorAction SilentlyContinue
    try {
        New-SmbMapping -LocalPath "U:" -RemotePath "\\192.168.222.20\Azure_Backups" -UserName "azuredatasync" -Password $synologyPassword -ErrorAction Stop -ErrorVariable evar
        Write-OutWithDateTime "Successfully connected to Synology"
    } catch {
        Write-OutWithDateTime "WARNING::::Unable to map drive to Synology"
        throw 
    }
}
function CopyFileToSynology([String]$fileNameWithPath){
    AttachToSynology
    DeleteOldFilesFromSynology
    try {
        Write-OutWithDateTime (-join("Trying Copy To Synology ",$fileNameWithPath))
        Copy-Item  $fileNameWithPath -Destination "U:\VM" -ErrorAction Stop -ErrorVariable $evar
        Write-OutWithDateTime (-join("Successfully Copied ".$fileNameWithPath))

        try {
            Remove-Item -Path $fileNameWithPath -Force -ErrorAction Stop -ErrorVariable evar
            Write-OutWithDateTime (-join("Successfully Removed File ",$fileNameWithPath))
        } catch {
            Write-OutWithDateTime (-join("WARNING::::Failed to remove ",$fileNameWithPath))
            throw
        }
    } catch {
        Write-OutWithDateTime ("WARNING::::Failed to copy to Synology ",$fileNameWithPath)
        throw
    }
}
function DeleteOldFilesFromSynology{
            Write-OutWithDateTime (-join("Deleting files older than 7 days from synology"))
        Get-ChildItem U:\VM -Recurse -File | Where {$_.CreationTime -lt (Get-Date).AddDays(-7)} | Remove-Item -Force
    
}
function CleanOutStagingDirectory{
    
        Write-OutWithDateTime (-join("Deleting files from staging directory h:\azusreDiskCopyStaging"))
        Get-ChildItem H:\AzureDiskCopyStaging -Recurse -File | Remove-Item -Force
    
}

[array]$listOfVMsToBackup = @('azu-nxp01','azu-nxp02','NXP05')
#[array]$listOfVMsToBackup = @('NXP05')
[array]$listOfDisks = @()
[String]$localFolder = "H:\AzureDiskCopyStaging"
[String]$disksResourceGroup = "NXPRESTOREDVMDISKS"
[String]$currentContainer=""
[String]$currentItem=""
[String]$synologyPassword="7V@7#s4nPQ&bB,4"
[String]$tenantid = "16153bae-9a0c-43d9-9983-f4fb30ad2161"
[String]$appid = "191afb43-9cbb-4db6-8b12-662ac8357ae9"
[String]$subscriptionid = "a3ad2c3a-2aa8-474b-98cc-1f343a44c55b"
#Connect-AzAccount

$rundateTime=get-date -format "yyyyMMdd_HHmmss"

start-transcript -path "C:\NexpointAzureVHDBackups\$rundateTime.log"
#

ConnectToAzPowershell
ConnectToAzureCLI
CleanOutStagingDirectory

$numberOfVMsToBackup=$listOfVMsToBackup.Count
Write-OutWithDateTime (-join("Number Of VMs Count = ", $numberOfVMsToBackup))
for (($vmNumber=0); $vmNumber -lt $numberOfVMsToBackup; $vmNumber++){
    $currentContainer=$listOfVMsToBackup[$vmNumber]
    $currentItem=$listOfVMsToBackup[$vmNumber]

    Write-OutWithDateTime (-join("vmNumber,currentContainer&item= ", $vmNumber, " ", $currentContainer))

    $currentRecoveryPointidName=GetLatestRecoveryPoint $currentContainer $currentItem
    if (!$currentRecoveryPointidName){
        Write-OutWithDateTime
        throw "Unable to get current Recovery Point ID Name"
    }

    if ($currentRecoveryPointidName){
        $currentRecoveryPointTime=GetRecoveryPointTime $currentRecoveryPointidName $currentContainer $currentItem
        $currentRecoveryPointDateTimeClean=GetCleanRecoveryPointDateTime $currentRecoveryPointTime
    }

    $containerLongName=GetBackupContainerLongName $currentContainer

    If (!$containerLongName) {
        Write-OutWithDateTime
        throw "Long Container Name Is Empty"
    }

    Write-OutWithDateTime (-join("Submitting Restore Job for : id ",$currentRecoveryPointidName," time ",$currentRecoveryPointTime, " clean ",$currentRecoveryPointDateTimeClean," longname ",$containerLongName))

    $RestoreJobOutput=RestoreTheDisks $containerLongName $currentItem $currentRecoveryPointidName
    if ($RestoreJobOutput){
        $RestoreJobGUID=GetRestoreJobGUID $RestoreJobOutput
    } else {
        Write-OutWithDateTime "WARNING::::Submit Job Failed to Submit. No Output"
        Throw "Submit Job Failed to Submit. No Output"
    }

    $JobStatus=GetStatusOfRestoreJob $RestoreJobGUID
    Write-OutWithDateTime (-join("Initial Job Status: ",$JobStatus))

    If ($JobStatus -eq "Completed"){
        Write-OutWithDateTime "Job Status Completed.  Continuing."
    } elseif ($JobStatus -eq "InProgress") {
        $counter=0
        Write-OutWithDateTime "Job Status Still InProgress.  Waiting 30 seconds. Counter=$counter"
        While ($counter -lt 10){
            Start-Sleep -Seconds 30
            $JobStatus=GetStatusOfRestoreJob $RestoreJobGUID
                if ($JobStatus -eq "InProgress"){
                    $counter++
                    Write-OutWithDateTime "Job Status Still InProgress.  Waiting 30 seconds. Counter=$counter"
                }
                elseif ($JobStatus -eq "Completed") {
                    $counter=20
                } else {
                    throw "WARNING::::Job Status Unknown Condition. Counter=$counter.  Exiting"
                }
        } 
    }

    if ($JobStatus -ne "Completed"){
        Write-OutWithDateTime (-join("Job Status ",$JobStatus))
        throw (-join("WARNING::::Job Status Not Completed.  Can't Continue ",$JobStatus))
    } 

    $listOfDisks=GetListOfDisks
    if ($listOfDisks) {
        foreach ($diskName in $listOfDisks){
            if ($listOfDisks.Count -gt 0){
                Write-OutWithDateTime (-join("Number of Disks to Download ",$listOfDisks.Count))
                Write-Output $listOfDisks | Format-Table
                Write-OutWithDateTime (-join("Now Processing Disk Named ",$diskName.Name))

                [string]$currentDiskToDownloadURL = EnableAndGetDownloadURL $disksResourceGroup $diskName.Name
                    if ($currentDiskToDownloadURL) {
                        Write-OutWithDateTime (-join(" Got Download URL -->",$currentDiskToDownloadURL))
                            [array]$blobBeingDownloaded=DownloadTheDiskAsVHD $currentDiskToDownloadURL $localFolder
                                if ($blobBeingDownloaded){
                                    Write-OutWithDateTime (-join("Downloaded Successfully ",$blobBeingDownloaded[0]," ",$blobBeingDownloaded[1]))
                                    [string]$newFileNameWithBackupDateAppended=$(GetJustDiskName $diskName.Name) + "_" + $currentRecoveryPointDateTimeClean
                                    [string]$resultOfFileRename=RenameDownloadedVHD $localFolder $blobBeingDownloaded[0] $newFileNameWithBackupDateAppended
                                    if ($resultOfFileRename) {
                                        RemoveDiskFromRG($diskName.Name)
                                        [string]$newFileNameWithPath=(-join($localFolder,"\",$newFileNameWithBackupDateAppended,".vhd")) 
                                        CopyFileToSynology $newFileNameWithPath
                                    }
                                    Write-OutWithDateTime (-join("File Rename ", $resultOfFileRename," ",$localFolder," ", $blobBeingDownloaded[0]," ", $newFileNameWithBackupDateAppended, " ",$resultOfFileRename))
                                } else {
                                    Write-OutWithDateTime (-join("WARNING::::Download Failed for ",$diskName.Name))
                                    Write-OutWithDateTime (-join("WARNING::::Moving On To Next Disk If There Is One"))
                                }
                    } else {
                        Write-OutWithDateTime (-join("WARNING::::Unable to get download URL for ",$diskName.Name))
                        break
                    }
            } else {
                Write-OutWithDateTime "WARNING::::Error ListOfDisks is 0"
                throw "ListOfDisks 0 Can't Continue"
            }
        }
    } else {
        Write-OutWithDateTime "WARNING::::ListOfDisks Variable Empty"
    }

}
Write-OutWithDateTime "Stop Time"
Stop-transcript

