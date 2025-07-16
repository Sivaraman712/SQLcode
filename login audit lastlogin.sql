USE [DBADB]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_ProcessLoginAudit]
    @FilePath NVARCHAR(4000)
AS
BEGIN
    SET NOCOUNT ON;

    -- Step 1: Insert login events from audit file
    INSERT INTO AuditLoginEvents (
        server_principal_name,
        event_time_ist,
        client_ip,
        application_name,
        file_name
    )
    SELECT 
        server_principal_name,
        event_time AT TIME ZONE 'UTC' AT TIME ZONE 'India Standard Time' AS event_time_ist,
        client_ip,
        application_name,
        file_name
    FROM sys.fn_get_audit_file(@FilePath, DEFAULT, DEFAULT)
    WHERE action_id = 'LGIS'
    ORDER BY event_time_ist DESC;

    -- Step 2: Update last login time for each principal
    MERGE LoginLastSeen AS target
    USING (
        SELECT server_principal_name, MAX(event_time_ist) AS last_login_time
        FROM AuditLoginEvents
        GROUP BY server_principal_name
    ) AS source
    ON target.server_principal_name = source.server_principal_name
    WHEN MATCHED THEN
        UPDATE SET last_login_time = source.last_login_time
    WHEN NOT MATCHED THEN
        INSERT (server_principal_name, last_login_time)
        VALUES (source.server_principal_name, source.last_login_time);

    -- Step 3: Clear temporary audit data to avoid duplication
    TRUNCATE TABLE dbo.AuditLoginEvents;

    -- Step 4: Move processed audit file to archive folder
    DECLARE @cmd NVARCHAR(4000) = 'MOVE "' + @FilePath + '" "C:\Sivaraman\Login\Processed\"';
    EXEC xp_cmdshell @cmd;

END
GO
 

 USE [DBADB]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

   CREATE PROCEDURE [dbo].[usp_ProcessAllLoginAuditFiles]
    @FolderPath NVARCHAR(4000)
AS
BEGIN
    SET NOCOUNT ON;

    -- Temp table to hold file names
    CREATE TABLE #Files (
        FileName NVARCHAR(4000)
    );

    -- Get list of .sqlaudit files in the folder
    INSERT INTO #Files (FileName)
    EXEC xp_cmdshell 'dir /b "C:\Sivaraman\Login\*.sqlaudit"';

    DECLARE @FileName NVARCHAR(4000);
    DECLARE @FullPath NVARCHAR(4000);

    DECLARE file_cursor CURSOR FOR
        SELECT FileName FROM #Files WHERE FileName IS NOT NULL;

    OPEN file_cursor;
    FETCH NEXT FROM file_cursor INTO @FileName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @FullPath = @FolderPath + @FileName;

        -- Call the login event processor
        EXEC dbo.usp_ProcessLoginAudit @FullPath;

        FETCH NEXT FROM file_cursor INTO @FileName;
    END

    CLOSE file_cursor;
    DEALLOCATE file_cursor;

    DROP TABLE #Files;
END
GO


 CREATE TABLE [dbo].[AuditLoginEvents](
 [server_principal_name] [nvarchar](256) NULL,
 [event_time_ist] [datetime] NULL,
 [client_ip] [nvarchar](50) NULL,
 [application_name] [nvarchar](256) NULL,
 [file_name] [nvarchar](512) NULL
) ON [PRIMARY]
GO
 

CREATE TABLE [dbo].[LoginLastSeen](
 [server_principal_name] [nvarchar](256) NOT NULL,
 [last_login_time] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
 [server_principal_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO