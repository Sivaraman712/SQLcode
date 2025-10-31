USE msdb;
GO

DECLARE @job_name NVARCHAR(128) = N'DBA Full Index Rebuild';
DECLARE @job_id UNIQUEIDENTIFIER;
 
-- Get the job ID
SELECT @job_id = job_id
FROM msdb.dbo.sysjobs 
WHERE name = @job_name;

IF @job_id IS NULL
BEGIN
    PRINT 'Full Rebuild Job not found.';
    RETURN;
END

-- Check if the job is currently running
IF EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobactivity AS sja
    WHERE sja.job_id = @job_id
      AND sja.stop_execution_date IS NULL
      AND sja.start_execution_date IS NOT NULL 
      AND sja.run_requested_date IS NOT NULL AND cast(sja.run_requested_date as date) = cast(getdate() as date)
)
BEGIN
    PRINT 'Full Rebuild Job is running. Stopping the job...';
    EXEC msdb.dbo.sp_stop_job @job_name = @job_name;
    EXEC msdb.dbo.sp_start_job @job_name = N'DBA Update Stats & Recompile';
    EXEC msdb.dbo.sp_start_job @job_name = N'DBA Index Maintenance Completed alert';
END
ELSE
BEGIN
    PRINT 'Full Rebuild Job is not running.';
END
GO


SET NOCOUNT ON;

DECLARE @IndexSessionRunning BIT = 1;

WHILE @IndexSessionRunning = 1
BEGIN
    IF EXISTS (
         SELECT 1
        FROM sys.dm_exec_requests r where r.command='ALTER INDEX'
    )
    BEGIN
        PRINT 'INDEX Maintenance in progress. Waiting...';
        WAITFOR DELAY '00:00:10';
    END
    ELSE
    BEGIN
        SET @IndexSessionRunning = 0;
    END
END



DECLARE  @xml nvarchar(max);

DECLARE @job_name NVARCHAR(128),
        @reportDate DATE = CAST(GETDATE() AS DATE),
        @yesterday DATE = CAST(GETDATE()-1  AS DATE),
        @twoDaysAgo DATE = CAST(GETDATE() - 2 AS DATE),
        @HadRebuildYesterday BIT,
        @HadRebuildTwoDaysAgo BIT;

    -- Check if rebuilds occurred on specific days
    SELECT @HadRebuildYesterday = 
        CASE WHEN EXISTS (
            SELECT 1 FROM DBADB..CommandLog WITH (NOLOCK)
            WHERE CAST(StartTime AS DATE) = @yesterday
              AND CommandType = 'ALTER_INDEX'
              AND EndTime IS NOT NULL
        ) THEN 1 ELSE 0 END;

    SELECT @HadRebuildTwoDaysAgo = 
        CASE WHEN EXISTS (
            SELECT 1 FROM DBADB..CommandLog WITH (NOLOCK)
            WHERE CAST(StartTime AS DATE) = @twoDaysAgo
              AND CommandType = 'ALTER_INDEX'
              AND EndTime IS NOT NULL
        ) THEN 1 ELSE 0 END;

    IF @HadRebuildYesterday = 0
    BEGIN
       SET @job_name = N'DBA Full Index Rebuild';
		 

        
    END
    ELSE IF @HadRebuildYesterday = 1 AND @HadRebuildTwoDaysAgo = 0
    BEGIN
       SET @job_name  = N'DBA Remaining Index Rebuild';
          

       
    END
    ELSE
    BEGIN
       
        RETURN;
    END
SELECT @xml = Cast((SELECT sj.[name] AS 'td',
'',
sh.step_name AS 'td',
'',
msdb.dbo.agent_datetime(sh.run_date,sh.run_time) AS 'td',
'',
CASE WHEN sh.run_duration > 235959
            THEN CAST((CAST(LEFT(CAST(sh.run_duration AS VARCHAR),
                LEN(CAST(sh.run_duration AS VARCHAR)) - 4) AS INT) / 24) AS VARCHAR)
                    + '.' + RIGHT('00' + CAST(CAST(LEFT(CAST(sh.run_duration AS VARCHAR),
                LEN(CAST(sh.run_duration AS VARCHAR)) - 4) AS INT) % 24 AS VARCHAR), 2)
                    + ':' + STUFF(CAST(RIGHT(CAST(sh.run_duration AS VARCHAR), 4) AS VARCHAR(6)), 3, 0, ':')
        ELSE STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(sh.run_duration AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
        END AS 'td',
'', 
CASE sh.[run_status] WHEN 0 THEN 'Failed'
						WHEN 1 THEN 'Succeeded'
						WHEN 2 THEN 'Retry'
						WHEN 3 THEN 'Canceled'
						WHEN 4 THEN 'Running' 
END AS 'td',
'',
SUBSTRING(sh.[message],1,100) AS 'td'
FROM msdb.dbo.sysjobhistory AS sh
INNER JOIN msdb.dbo.sysjobs AS sj ON sh.job_id = sj.job_id
WHERE sj.[name] = @job_name AND sh.step_id <> 0
AND sh.run_date = (SELECT MAX(run_date) FROM msdb.dbo.sysjobhistory WHERE job_id = sj.job_id)
AND msdb.dbo.agent_datetime(sh.run_date,0) = cast(cast(getdate() as date) as datetime)
FOR xml path('tr'), elements) AS NVARCHAR(max))
Declare @body nvarchar(max)
SET @body =
'<html>
	<head>
		<style>
			table, th, td 
			{
				border: 1px solid black;
				border-collapse: collapse;
				text-align: center;
			}
		</style>
	</head>
	<body>
		<H2>
		Index Maintenance Job Status 
		</H2>
		<table> 
			<tr>
				<th> Job Name </th> <th> Step Name </th> <th> Start Time </th> <th> Duration (d.HH:MM:SS) </th> <th> Status </th> <th> Message </th>
			</tr>'
			SET @body = @body + @xml + '
		</table>
	</body>
</html>'
if(@xml is not null)
BEGIN
EXEC msdb.dbo.Sp_send_dbmail
@profile_name = 'DBMAIL',
@body = @body,
@body_format ='html',
@recipients = 'mssqlalerts@geopits.com',
@blind_copy_recipients='mssqltechsupport@geopits.com',
@subject = 'Index Maintenance Job Status';
END
SET NOCOUNT OFF

USE [DBADB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Naresh
-- Create date: 26-03-2025
-- Description:	Notification SP
-- =============================================
CREATE PROCEDURE [dbo].[Index_maintenance_notify]
	
AS
BEGIN
	DECLARE  @xml nvarchar(max)
SELECT @xml = Cast((SELECT sj.[name] AS 'td','',sh.step_name AS 'td','',msdb.dbo.agent_datetime(sh.run_date,sh.run_time) AS 'td','',
CASE WHEN sh.run_duration > 235959
            THEN CAST((CAST(LEFT(CAST(sh.run_duration AS VARCHAR),
                LEN(CAST(sh.run_duration AS VARCHAR)) - 4) AS INT) / 24) AS VARCHAR)
                    + '.' + RIGHT('00' + CAST(CAST(LEFT(CAST(sh.run_duration AS VARCHAR),
                LEN(CAST(sh.run_duration AS VARCHAR)) - 4) AS INT) % 24 AS VARCHAR), 2)
                    + ':' + STUFF(CAST(RIGHT(CAST(sh.run_duration AS VARCHAR), 4) AS VARCHAR(6)), 3, 0, ':')
        ELSE STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(sh.run_duration AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
        END AS 'td',
'', 
CASE sh.[run_status] WHEN 0 THEN 'Failed'
						WHEN 1 THEN 'Succeeded'
						WHEN 2 THEN 'Retry'
						WHEN 3 THEN 'Canceled'
						WHEN 4 THEN 'Running' 
END AS 'td',
'',
SUBSTRING(sh.[message],1,100) AS 'td'
FROM msdb.dbo.sysjobhistory AS sh
INNER JOIN msdb.dbo.sysjobs AS sj ON sh.job_id = sj.job_id
WHERE sj.[name] = 'DBA_Index Optimize_Daily' AND sh.step_id = 2
AND sh.run_date = (SELECT MAX(run_date) FROM msdb.dbo.sysjobhistory WHERE job_id = sj.job_id)
AND msdb.dbo.agent_datetime(sh.run_date,0) = cast(cast(getdate() as date) as datetime)
FOR xml path('tr'), elements) AS NVARCHAR(max))

DECLARE  @xml1 nvarchar(max)
SELECT @xml1 = Cast((select DatabaseName AS 'td','', ObjectName AS 'td','', IndexName AS 'td','', StartTime AS 'td','', EndTime AS 'td','',
DATEDIFF(SECOND, StartTime, EndTime) AS 'td','', (ExtendedInfo.value('(/ExtendedInfo/PageCount)[1]', 'bigint') * 8)/1024 AS 'td','',
ExtendedInfo.value('(/ExtendedInfo/Fragmentation)[1]', 'numeric(10,2)') AS 'td','', ExtendedInfo.value('(/ExtendedInfo/PageCount)[1]', 'bigint') AS 'td'
from DBADB.dbo.CommandLog where cast(StartTime as date) = cast(GETDATE() as date) and CommandType = 'ALTER_INDEX' 
FOR xml path('tr'), elements) AS NVARCHAR(max))

DECLARE  @xml2 nvarchar(max)
SELECT @xml2 = Cast((select DatabaseName AS 'td','', ObjectName AS 'td','', IndexName AS 'td','', StartTime AS 'td','', EndTime AS 'td','',
DATEDIFF(SECOND, StartTime, EndTime) AS 'td','', ExtendedInfo.value('(/ExtendedInfo/RowCount)[1]', 'bigint') AS 'td','',
ExtendedInfo.value('(/ExtendedInfo/ModificationCounter)[1]', 'int') AS 'td'
from DBADB.dbo.CommandLog where cast(StartTime as date) = cast(GETDATE() as date) and CommandType = 'UPDATE_STATISTICS' 
FOR xml path('tr'), elements) AS NVARCHAR(max))

Declare @body nvarchar(max)
SET @body =
'<html>
	<head>
		<style>
			table, th, td 
			{
				border: 1px solid black;
				border-collapse: collapse;
				text-align: center;
			}
		</style>
	</head>
	<body>
		<H2>
		Index Maintenance Status
		</H2>
		<table> 
			<tr>
				<th> Job Name </th> <th> Step Name </th> <th> Start Time </th> <th> Duration (d.HH:MM:SS) </th> <th> Status </th> <th> Message </th>
			</tr>'
			SET @body = @body + @xml + '
		</table>
		<br/>
		<H2>
		Index Maintenance Details
		</H2>
		<table> 
			<tr>
				<th> Database Name </th> <th> Table Name </th> <th> Index Name </th> <th> Start Time </th> <th> End Time </th> <th> Duration (sec) </th> <th> Index Size(mb) </th> <th> Fragmentation </th> <th> Page count </th>
			</tr>'
			SET @body = @body + @xml1 + '
		</table>
		<br/>
		<H2>
		Update Stats Details
		</H2>
		<table> 
			<tr>
				<th> Database Name </th> <th> Table Name </th> <th> Index Name </th> <th> Start Time </th> <th> End Time </th> <th> Duration (sec) </th> <th> Row Count </th> <th> Modification Counter </th>
			</tr>'
			SET @body = @body + @xml2 + '
		</table>
	</body>
</html>'
if(@xml is not null)
BEGIN
EXEC msdb.dbo.Sp_send_dbmail
@profile_name = 'DBMail',
@body = @body,
@body_format ='html',
@recipients = 'mssqlalerts@geopits.com;',
@subject = 'Index Maintenance Status';
END
END
