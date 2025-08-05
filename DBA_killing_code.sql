
IF OBJECT_ID('DBADB..KilledSessionsLog', 'U') IS  NULL
CREATE  TABLE KilledSessionsLog (
    SessionID INT,
    LoginName NVARCHAR(128),
    HostName NVARCHAR(128),
    ProgramName NVARCHAR(128),
    Status NVARCHAR(30),
    LastRequestEndTime DATETIME,
    LoginTime DATETIME,
    SqlText NVARCHAR(MAX),
    ObjectName NVARCHAR(512),
    WaitType NVARCHAR(60),
    DatabaseName NVARCHAR(128),
    OpenTranCount INT
);




DECLARE @session_id INT;
DECLARE @kill_cmd NVARCHAR(100);

DECLARE cur CURSOR FAST_FORWARD FOR
SELECT DISTINCT s.session_id
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
WHERE 
    s.session_id > 50
    AND s.is_user_process = 1
    AND s.session_id <> @@SPID
    AND r.command='SELECT'
    AND s.login_name ='IISuser'  -- Change the username on the based on server login name
    AND (
        -- Running or sleeping more than 90 mins 
        DATEDIFF(MINUTE, s.last_request_end_time, GETDATE()) >= 90
        
    );

OPEN cur;
FETCH NEXT FROM cur INTO @session_id;

WHILE @@FETCH_STATUS = 0
BEGIN
    INSERT INTO dbadb.dbo.KilledSessionsLog (
        SessionID, LoginName, HostName, ProgramName, Status, LastRequestEndTime, LoginTime, SqlText, ObjectName, WaitType, DatabaseName, OpenTranCount
    )
    SELECT 
        s.session_id,
        s.login_name,
        s.host_name,
        s.program_name,
        s.status,
        s.last_request_end_time,
        s.login_time,
        st.text AS SqlText,
        COALESCE(
            QUOTENAME(DB_NAME(st.dbid)) + N'.' + 
            QUOTENAME(OBJECT_SCHEMA_NAME(st.objectid, st.dbid)) + N'.' + 
            QUOTENAME(OBJECT_NAME(st.objectid, st.dbid)), 
            'Query'
        ) AS ObjectName,
        r.wait_type,
        DB_NAME(s.database_id) AS DatabaseName,
        s.open_transaction_count
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
    LEFT JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
    OUTER APPLY sys.dm_exec_sql_text(
        CASE 
            WHEN r.sql_handle IS NOT NULL THEN r.sql_handle
            ELSE c.most_recent_sql_handle
        END
    ) AS st
    WHERE s.session_id = @session_id;

    PRINT 'Killing session: ' + CAST(@session_id AS VARCHAR(10));

    SET @kill_cmd = 'KILL ' + CAST(@session_id AS VARCHAR(10));
    EXEC sp_executesql @kill_cmd;

    FETCH NEXT FROM cur INTO @session_id;
END

CLOSE cur;
DEALLOCATE cur;
