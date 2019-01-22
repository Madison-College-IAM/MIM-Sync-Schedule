# http://konab.com/scheduling-mim-advanced-options/
############
# PARAMETERS
############
param([parameter(Mandatory=$true)] [string]$RunType)
 
Import-Module sqlps

If ($RunType -eq "Delta")
    {
    $ImportSyncStage = 
        @(
        @{MAName="SQLMA-SD";ProfileToRun="DI";};
        @{MAName="SQLMA-SD";ProfileToRun="DS";};
        );

    $ImportAsJob = 
        @(
        @{MAName="MIMMA";ProfileToRun="DI";};
        @{MAName="ADMA-Main";ProfileToRun="DI";};
        @{MAName="ADMA-DM-Z";ProfileToRun="DI";};
        );
 
    $SyncProfilesOrder = 
        @(
         @{MAName="SQLMA-SD";ProfileToRun="DS";};
        #@{MAName="ADMA-Main";profilesToRun=@("DS");};
        #@{MAName="ADMA-DM-Z";profilesToRun=@("DS");};
        #@{MAName="MIMMA";profilesToRun=@("DS");};
        @{MAName="MIMMA";profilesToRun=@("EX";"Sleep:15";"DI";"DS");};
        );
 
    $ExportAsJob = 
        @(
        @{MAName="ADMA-Main";ProfileToRun="EX";};
        @{MAName="ADMA-DM-Z";ProfileToRun="EX";};
        @{MAName="MIMMA";ProfileToRun="EX";};
        );
    #Log "Info" "Running Delta Syncs"
    }
ElseIf ($RunType -eq "Full")
    {
    $ImportSyncStage = 
        @(
        @{MAName="SQLMA-SD";ProfileToRun="FI";};
        @{MAName="SQLMA-SD";ProfileToRun="FS";};
        );

    $ImportAsJob = 
        @(
        @{MAName="ADMA-Main";ProfileToRun="FI";};
        @{MAName="ADMA-DM-Z";ProfileToRun="FI";};
        @{MAName="MIMMA";ProfileToRun="FI";};
        );
 
    $SyncProfilesOrder = 
        @(
        @{MAName="ADMA-Main";profilesToRun=@("FS");};
        @{MAName="ADMA-DM-Z";profilesToRun=@("FS");};
        @{MAName="MIMMA";profilesToRun=@("EX";"Sleep:15";"DI";"FS");};
        );
 
    $ExportAsJob = 
        @(
        @{MAName="ADMA-Main";ProfileToRun="EX";};
        @{MAName="ADMA-DM-Z";ProfileToRun="EX";};
        @{MAName="MIMMA";ProfileToRun="EX";};
        );
    #Log "Info" "Running Full Syncs"
    }
Else
    {
    "No run type selected"
    Break
    }

$Query = @"
/**** If delta table exists from previous run, drop it ****/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_NAME = N'Identities_Delta')
BEGIN
  DROP TABLE Identities_Delta;
END

USE [StagingDirectory]
GO

/****** Object:  Table [dbo].[Identities]    Script Date: 4/7/2016 8:11:36 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

/**** Create new delta table ****/

CREATE TABLE [dbo].[Identities_Delta](
	[identityID] [bigint] IDENTITY(1,1) NOT NULL,
	[accountName] [varchar](50) NULL,
	[email] [varchar](100) NULL,
	[changeType] [varchar](50) NULL,
	[city] [varchar](50) NULL,
	[company] [varchar](255) NULL,
	[costCenter] [varchar](255) NULL,
	[department] [nvarchar](100) NULL,
	[displayName] [varchar](50) NULL,
	[employeeEndDate] [varchar](50) NULL,
	[employeeID] [varchar](50) NULL,
	[employeeStartDate] [varchar](50) NULL,
	[employeeStatus] [varchar](50) NULL,
	[employeeType] [varchar](50) NULL,
	[firstName] [varchar](100) NULL,
	[isContingent] [varchar](50) NULL,
	[isFaculty] [varchar](50) NULL,
	[isRetired] [varchar](50) NULL,
	[jobCode] [nvarchar](255) NULL,
	[jobTitle] [nvarchar](255) NULL,
	[initials] [varchar](50) NULL,
	[ipPhone] [varchar](50) NULL,
	[lastName] [varchar](50) NULL,
	[manager] [nvarchar](255) NULL,
	[managerID] [nvarchar](255) NULL,
	[mobilePhone] [varchar](255) NULL,
	[officeLocation] [varchar](255) NULL,
	[officeLocationCode] [varchar](50) NULL,
	[officePhone] [varchar](50) NULL,
	[personalEmail] [varchar](255) NULL,
	[positionID] [varchar](255) NULL,
	[positionTime] [nvarchar](255) NULL,
	[postalCode] [varchar](50) NULL,
	[roomNumber] [varchar](255) NULL,
	[st] [varchar](50) NULL,
	[streetAddress] [varchar](255) NULL,
	[contractEndDate] [varchar](50) NULL,
	[employeeNumber] [varchar](100) NULL,
	[isActivated] [varchar](50) NULL,
	[isFERPA] [varchar](50) NULL,
	[preferredName] [varchar](255) NULL,
	[dateOfBirth] [varchar](50) NULL,
    [whenCreated] [datetime] NULL,
    [doNotSync] [nvarchar](10) NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

SET IDENTITY_INSERT Identities_Delta OFF
GO

/**** Insert all records that do not exist in the archive table ****/

INSERT INTO	Identities_Delta 
		(
		accountName,
		email,
		changeType,
		city,
		company,
		costCenter,
		department,
		displayName,
		employeeEndDate,
		employeeID,
		employeeStartDate,
		employeeStatus,
		employeeType,
		firstName,
		isContingent,
		isFaculty,
		isRetired,
		jobCode,
		jobTitle,
		initials,
		ipPhone,
		lastName,
		manager,
		managerID,
		mobilePhone,
		officeLocation,
		officeLocationCode,
		officePhone,
		personalEmail,
		positionID,
		positionTime,
		postalCode,
		roomNumber,
		st,
		streetAddress,
		contractEndDate,
		employeeNumber,
		isActivated,
		isFERPA,
		preferredName,
		dateOfBirth
		)
SELECT	s.accountName,
		s.email,
		'Add' AS ChangeType,
		s.city,
		s.company,
		s.costCenter,
		s.department,
		s.displayName,
		s.employeeEndDate,
		s.employeeID,
		s.employeeStartDate,
		s.employeeStatus,
		s.employeeType,
		s.firstName,
		s.isContingent,
		s.isFaculty,
		s.isRetired,
		s.jobCode,
		s.jobTitle,
		s.initials,
		s.ipPhone,
		s.lastName,
		s.manager,
		s.managerID,
		s.mobilePhone,
		s.officeLocation,
		s.officeLocationCode,
		s.officePhone,
		s.personalEmail,
		s.positionID,
		s.positionTime,
		s.postalCode,
		s.roomNumber,
		s.st,
		s.streetAddress,
		s.contractEndDate,
		s.employeeNumber,
		s.isActivated,
		s.isFERPA,
		s.preferredName,
		s.dateOfBirth
FROM	dbo.Identities_Archive AS a RIGHT OUTER JOIN
        dbo.Identities AS s ON a.employeeNumber = s.employeeNumber
WHERE   (a.employeeNumber IS NULL)

/**** Insert all records that exist in the archive table, but have changed ****/

INSERT INTO	Identities_Delta 
		(
		accountName,
		email,
		changeType,
		city,
		company,
		costCenter,
		department,
		displayName,
		employeeEndDate,
		employeeID,
		employeeStartDate,
		employeeStatus,
		employeeType,
		firstName,
		isContingent,
		isFaculty,
		isRetired,
		jobCode,
		jobTitle,
		initials,
		ipPhone,
		lastName,
		manager,
		managerID,
		mobilePhone,
		officeLocation,
		officeLocationCode,
		officePhone,
		personalEmail,
		positionID,
		positionTime,
		postalCode,
		roomNumber,
		st,
		streetAddress,
		contractEndDate,
		employeeNumber,
		isActivated,
		isFERPA,
		preferredName,
		dateOfBirth
		)
SELECT	s.accountName,
		s.email,
		'Modify' AS ChangeType,
		s.city,
		s.company,
		s.costCenter,
		s.department,
		s.displayName,
		s.employeeEndDate,
		s.employeeID,
		s.employeeStartDate,
		s.employeeStatus,
		s.employeeType,
		s.firstName,
		s.isContingent,
		s.isFaculty,
		s.isRetired,
		s.jobCode,
		s.jobTitle,
		s.initials,
		s.ipPhone,
		s.lastName,
		s.manager,
		s.managerID,
		s.mobilePhone,
		s.officeLocation,
		s.officeLocationCode,
		s.officePhone,
		s.personalEmail,
		s.positionID,
		s.positionTime,
		s.postalCode,
		s.roomNumber,
		s.st,
		s.streetAddress,
		s.contractEndDate,
		s.employeeNumber,
		s.isActivated,
		s.isFERPA,
		s.preferredName,
		s.dateOfBirth
FROM	dbo.Identities_Archive AS a INNER JOIN
        dbo.Identities AS s ON a.employeeNumber = s.employeeNumber
WHERE   a.accountName <> s.accountName OR
		a.email <> s.email OR
		a.city <> s.city OR
		a.company <> s.company OR
		a.costCenter <> s.costCenter OR
		a.department <> s.department OR
		a.displayName <> s.displayName OR
		a.employeeEndDate <> s.employeeEndDate OR
		a.employeeID <> s.employeeID OR
		a.employeeStartDate <> s.employeeStartDate OR
		a.employeeStatus <> s.employeeStatus OR
		a.employeeType <> s.employeeType OR
		a.firstName <> s.firstName OR
		a.isContingent <> s.isContingent OR
		a.isFaculty <> s.isFaculty OR
		a.isRetired <> s.isRetired OR
		a.jobCode <> s.jobCode OR
		a.jobTitle <> s.jobTitle OR
		a.initials <> s.initials OR
		a.ipPhone <> s.ipPhone OR
		a.lastName <> s.lastName OR
		a.manager <> s.manager OR
		a.managerID <> s.managerID OR
		a.mobilePhone <> s.mobilePhone OR
		a.officeLocation <> s.officeLocation OR
		a.officeLocationCode <> s.officeLocationCode OR
		a.officePhone <> s.officePhone OR
		a.personalEmail <> s.personalEmail OR
		a.positionID <> s.positionID OR
		a.positionTime <> s.positionTime OR
		a.postalCode <> s.postalCode OR
		a.roomNumber <> s.roomNumber OR
		a.st <> s.st OR
		a.streetAddress <> s.streetAddress OR
		a.contractEndDate <> s.contractEndDate OR
		a.employeeNumber <> s.employeeNumber OR
		a.isActivated <> s.isActivated OR
		a.isFERPA <> s.isFERPA OR
		a.preferredName <> s.preferredName OR
		a.dateOfBirth <> s.dateOfBirth

/**** Drop and recreate achieve table by making a copy of the identities table ****/

IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
           WHERE TABLE_NAME = N'Identities_Archive')
BEGIN
  DROP TABLE Identities_Archive;
END

SELECT * INTO Identities_Archive
FROM Identities

--TRUNCATE TABLE Identities_Delta
"@

$Global:date = get-date -uformat "%Y-%m-%d"
$Global:FilePath = "C:\Scripts"
$Global:Logfile = $FilePath + "\"+$date+"-Exchange-Provisioning.txt"
     
############
# DATA
############
$MAs = @(get-wmiobject -class "MIIS_ManagementAgent" -namespace "root\MicrosoftIdentityIntegrationServer" -computername ".")
 
############
# FUNCTIONs
############

Function Log($EntryType,$entry)
        {
        $datetime = get-date -uformat "%Y-%m-%d-%H:%M:%S"
        if (-not(get-item $Logfile -ea 0))
            {
            $DateTime+"-"+$EntryType+"-"+$Entry | Out-File $Logfile
            }
            Else
            {
            $DateTime+"-"+$EntryType+"-"+$Entry | Out-File $Logfile -Append
            }
        }
 
function RunFIMAsJob
    {
    param([string]$MAName, [string]$Profile)
    Start-Job -Name $MAName -ArgumentList $MAName,$Profile -ScriptBlock {
        param($MAName,$Profile)
        $MA = (get-wmiobject -class "MIIS_ManagementAgent" -namespace "root\MicrosoftIdentityIntegrationServer" -computername "." -Filter "Name='$MAName'")
        $return = $MA.Execute($Profile)
        (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": Finished : " + $MAName + " : " + $Profile + " : " + $return.ReturnValue
        #Log "Info" ": Finished : " + $MAName + " : " + $Profile + " : " + $return.ReturnValue
        }
    }

Function Create-DeltaTable
    {
    $Instance = "idmdbprd01\mimstage"
    $DataBase = "stagingdirectory"


    Invoke-Sqlcmd `
        -ServerInstance $Instance `
        -Database $DataBase `
        -query $Query
    } 

Function Send-UserReport
    {
    Send-MailMessage `
        -to "UMTeamDGnestedTest@madisoncollege.edu" `
        -From "Hanson, Joseph D <jdhanson1@madisoncollege.edu>" `
        -Body "FIM Sync Complete" `
        -Subject "FIM Sync Complete" `
        -SmtpServer "smtp.madisoncollege.edu" `
    }

############
# PROGRAM
############
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": Starting Schedule"

Log "Info" "Starting Synchronization Schedule"

(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Creating SQL Delta Table"
Log "Info" "Creating SQL Delta Table"

Create-DeltaTable

#Import Sync (Stage Only)
foreach($MAToRun in $ImportSyncStage)
    {
    foreach($profileName in $MAToRun.profileToRun)
        {
        if($profileName.StartsWith("Sleep"))
            {Start-Sleep -Seconds $profileName.Split(":")[1]}
        elseif($profileName.StartsWith("Script"))
            {& ($scriptpath +"\"+ ($profileName.Split(":")[1]))}
        else
            {
            $return = ($MAs | ?{$_.Name -eq $MAToRun.MAName}).Execute($profileName)
            (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": " + $MAToRun.MAName + " : " + $profileName + " : " + $return.ReturnValue
            Log "Info" "Starting : $($MAToRun.MAName) : $($profileName) : $($return.ReturnValue)"
            }
        }
    }

#ImportAsJob
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Starting Import Jobs"
Log "Info" "Starting Import Jobs"

foreach($MAToRun in $ImportAsJob)
    {
        (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": Starting : " + $MAToRun.MAName + " : " + $MAToRun.ProfileToRun
        Log "Info" "Starting : $($MAToRun.MAName) : $($MAToRun.ProfileToRun)"
        
        $void = RunFIMAsJob $MAToRun.MAName $MAToRun.ProfileToRun
    }
Get-Job | Wait-Job | Receive-Job -Keep
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Finished Import Jobs"
Log "Info" "Finishing Import Jobs"
 
#Removing Jobs to release resources
Get-Job | Remove-Job
 

#Sync (not as job)
foreach($MAToRun in $SyncProfilesOrder)
    {
    foreach($profileName in $MAToRun.profilesToRun)
        {
        if($profileName.StartsWith("Sleep"))
            {Start-Sleep -Seconds $profileName.Split(":")[1]}
        elseif($profileName.StartsWith("Script"))
            {& ($scriptpath +"\"+ ($profileName.Split(":")[1]))}
        else
            {
            $return = ($MAs | ?{$_.Name -eq $MAToRun.MAName}).Execute($profileName)
            (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": " + $MAToRun.MAName + " : " + $profileName + " : " + $return.ReturnValue
            Log "Info" "Starting : $($MAToRun.MAName) : $($profileName) : $($return.ReturnValue)"
            }
        }
    }
 
#ExportAsJob
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Starting ExportJobs"
foreach($MAToRun in $ExportAsJob)
    {
        (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": Starting : " + $MAToRun.MAName + " : " + $MAToRun.ProfileToRun
        Log "Info" ": Starting : $($MAToRun.MAName) : $($MAToRun.ProfileToRun)"
        $void = RunFIMAsJob $MAToRun.MAName $MAToRun.ProfileToRun
    }
Get-Job | Wait-Job | Receive-Job -Keep
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Finished ExportJobs"
Log "Info" "Finished ExportJobs"

#ImportAsJob (Confirming Input)
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Starting Confirming Import Jobs"
Log "Info" "Starting Import Jobs"

foreach($MAToRun in $ImportAsJob)
    {
        (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": Starting : " + $MAToRun.MAName + " : " + $MAToRun.ProfileToRun
        Log "Info" "Starting : $($MAToRun.MAName) : $($MAToRun.ProfileToRun)"
        
        $void = RunFIMAsJob $MAToRun.MAName $MAToRun.ProfileToRun
    }
Get-Job | Wait-Job | Receive-Job -Keep
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') +": Finished Import Jobs"
Log "Info" "Finishing Import Jobs"

#Removing Jobs to release resources
Get-Job | Remove-Job
 
(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + ": Finished Schedule"
Log "Info" "Finished Schedule"

Log "Info" "############ Finished ################"

if ($RunType -eq "Full")
    {
    Send-UserReport
    }