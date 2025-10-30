USE [DBADB]
GO



CREATE TABLE [dbo].[BlockedQueriesInfo](
	[ServerName] [nvarchar](100) NULL,
	[BlockedSessionID] [int] NULL,
	[StartTime] [datetime] NULL,
	[WaitingInMinutes] [float] NULL,
	[WaitType] [nvarchar](50) NULL,
	[BlockingSessionID] [int] NULL,
	[QueryWaiting] [nvarchar](max) NULL,
	[logdate] [datetime] NULL,
	[BlockingQuery] [nvarchar](max) NULL,
	[is_closed] [tinyint] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[BlockedQueriesInfo] ADD  DEFAULT (getdate()) FOR [logdate]
GO

ALTER TABLE [dbo].[BlockedQueriesInfo] ADD  DEFAULT ((0)) FOR [is_closed]
GO




--step 1

SET ANSI_NULLS, QUOTED_IDENTIFIER ON;

IF OBJECT_ID('tempdb..#BlockingInfo') IS NOT NULL DROP TABLE #BlockingInfo;
IF OBJECT_ID('tempdb..#BlockingInfo1') IS NOT NULL DROP TABLE #BlockingInfo1;

-- Create the temporary tables
CREATE TABLE #BlockingInfo (
    head_blocker_session_id INT,
    blocking_queries_count INT,
    head_blocker_query NVARCHAR(max));

IF OBJECT_ID('dbadb..BlockedQueriesInfo') IS NULL

CREATE  TABLE dbadb..BlockedQueriesInfo (
    ServerName NVARCHAR(100),
    BlockedSessionID INT,
    StartTime DATETIME,
    WaitingInMinutes FLOAT,
    WaitType NVARCHAR(50),
    BlockingSessionID INT,
    QueryWaiting NVARCHAR(max),
    BlockingQuery NVARCHAR(max),
	[is_closed] [tinyint] NULL default (0),
	logdate datetime default getdate()
);

IF OBJECT_ID('dbadb..HeadBlockingInfo') IS NULL
CREATE TABLE dbadb..HeadBlockingInfo (
     host_name NVARCHAR(100),
	login_name NVARCHAR(100),
	duration NVARCHAR(100),
	head_blocker_session_id INT,
    blocking_queries_count INT,
    head_blocker_query NVARCHAR(max),
	logdate datetime default getdate()
,is_closed tinyint DEFAULT 0);

INSERT INTO dbadb..BlockedQueriesInfo (ServerName, BlockedSessionID,QueryWaiting, StartTime, WaitingInMinutes, WaitType, BlockingSessionID, BlockingQuery)
SELECT 
    DISTINCT @@SERVERNAME AS [Server Name], 
    b.session_id AS [Blocked Session ID], 
    t.[text] AS [Query Waiting to Execute],
    r.start_time, 
    
    (b.wait_duration_ms / 1000) / 60 AS [Waiting in Minutes],
    b.wait_type AS [Wait Type],
    
    b.blocking_session_id AS [Blocking Session ID], 
    t1.[text] AS [Blocking Queries]
FROM 
    sys.dm_os_waiting_tasks b 
INNER JOIN 
    sys.dm_exec_requests r ON r.session_id = b.session_id and r.session_id>50
INNER JOIN 
    sys.dm_exec_requests r1 ON r1.session_id = b.blocking_session_id and r1.session_id>50
OUTER APPLY 
    sys.dm_exec_sql_text(r.sql_handle) t 
OUTER APPLY 
    sys.dm_exec_sql_text(r1.sql_handle) t1 
WHERE 
    b.blocking_session_id <> 0 and b.blocking_session_id <>b.session_id ;

-- Insert data into #BlockingInfo
WITH cteHead AS (
    SELECT 
        sess.session_id, 
        req.request_id, 
        LEFT(ISNULL(req.wait_type, ''), 50) AS wait_type,
        LEFT(ISNULL(req.wait_resource, ''), 40) AS wait_resource, 
        LEFT(req.last_wait_type, 50) AS last_wait_type,
        sess.is_user_process, 
        req.cpu_time AS request_cpu_time, 
        req.logical_reads AS request_logical_reads,
        req.reads AS request_reads, 
        req.writes AS request_writes, 
        req.wait_time, 
        req.blocking_session_id,
        sess.memory_usage,
        sess.cpu_time AS session_cpu_time, 
        sess.reads AS session_reads, 
        sess.writes AS session_writes, 
        sess.logical_reads AS session_logical_reads,
        CONVERT(decimal(5,2), req.percent_complete) AS percent_complete, 
        req.estimated_completion_time AS est_completion_time,
        req.start_time AS request_start_time, 
        LEFT(req.status, 15) AS request_status, 
        req.command,
        req.plan_handle, 
        req.sql_handle, 
        req.statement_start_offset, 
        req.statement_end_offset, 
        conn.most_recent_sql_handle,
        LEFT(sess.status, 15) AS session_status, 
        sess.group_id, 
        req.query_hash, 
        req.query_plan_hash
    FROM sys.dm_exec_sessions AS sess
    LEFT OUTER JOIN sys.dm_exec_requests AS req 
        ON sess.session_id = req.session_id and req.session_id>50
    LEFT OUTER JOIN sys.dm_exec_connections AS conn 
        ON conn.session_id = sess.session_id and conn.session_id>50
		   

),
cteBlockingHierarchy AS (
    SELECT 
        head.session_id AS head_blocker_session_id, 
        head.session_id AS session_id, 
        head.blocking_session_id,
        head.wait_type, 
        head.wait_time, 
        head.wait_resource, 
        head.statement_start_offset, 
        head.statement_end_offset,
        head.plan_handle, 
        head.sql_handle, 
        head.most_recent_sql_handle, 
        0 AS Level
    FROM cteHead AS head
    WHERE (head.blocking_session_id IS NULL OR head.blocking_session_id = 0)
    AND head.session_id IN (SELECT DISTINCT blocking_session_id FROM cteHead WHERE blocking_session_id != 0)
    UNION ALL
    SELECT 
        h.head_blocker_session_id, 
        blocked.session_id, 
        blocked.blocking_session_id, 
        blocked.wait_type,
        blocked.wait_time, 
        blocked.wait_resource, 
        h.statement_start_offset, 
        h.statement_end_offset,                                                                                                                                                                                                                                                                               
        h.plan_handle, 
        h.sql_handle, 
        h.most_recent_sql_handle, 
        [Level] + 1
    FROM cteHead AS blocked
    INNER JOIN cteBlockingHierarchy AS h 
        ON h.session_id = blocked.blocking_session_id 
        AND h.session_id != blocked.session_id -- Avoid infinite recursion for latch type of blocking
    WHERE h.wait_type COLLATE Latin1_General_BIN NOT IN ('EXCHANGE', 'CXPACKET') 
        OR h.wait_type IS NULL
)
INSERT INTO #BlockingInfo (head_blocker_session_id, blocking_queries_count, head_blocker_query)
SELECT 
    bh.head_blocker_session_id,
    COUNT(bh.session_id)  AS blocking_queries_count,
    txt.text AS head_blocker_query
	

FROM cteBlockingHierarchy AS bh
OUTER APPLY sys.dm_exec_sql_text(ISNULL(bh.sql_handle, bh.most_recent_sql_handle)) AS txt
where bh.head_blocker_session_id<>bh.session_id
GROUP BY bh.head_blocker_session_id, txt.text;

 -- select * from #BlockingInfo

CREATE TABLE #BlockingInfo1 (
    session_id INT,
    host_name NVARCHAR(100),
	login_name NVARCHAR(100),
	);

insert into #BlockingInfo1(session_id,host_name,login_name)
SELECT s.session_id, s.host_name,s.login_name
FROM sys.dm_exec_sessions s 
LEFT OUTER JOIN sys.dm_exec_requests r on r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text (r.sql_handle) t
OUTER APPLY sys.dm_exec_input_buffer(s.session_id, NULL) AS ib
WHERE s.session_id in   (select head_blocker_session_id from #BlockingInfo) 
ORDER BY   r.blocking_session_id desc, r.session_id

insert  into dbadb..HeadBlockingInfo(head_blocker_session_id ,blocking_queries_count , head_blocker_query,host_name ,login_name ,duration )
SELECT 
    s.head_blocker_session_id, 
    s.blocking_queries_count, 
    s.head_blocker_query,
    t.host_name, 
    t.login_name,    
    CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, MAX(ses.last_request_start_time), GETDATE()), 0), 108) AS duration
FROM 
    #BlockingInfo s 
INNER JOIN 
    #BlockingInfo1 t ON s.head_blocker_session_id = t.session_id
INNER JOIN 
    sys.dm_exec_sessions ses ON t.session_id = ses.session_id
WHERE s.head_blocker_session_id > 50 AND s.head_blocker_query is not null
GROUP BY 
    s.head_blocker_session_id, 
    s.blocking_queries_count, 
    s.head_blocker_query, 
    t.host_name, 
    t.login_name
	HAVING 
    DATEDIFF(SECOND, MAX(ses.last_request_start_time), GETDATE()) >= 180
	order by duration desc;
    


SET ANSI_NULLS, QUOTED_IDENTIFIER OFF;
GO

--Step 2

SET ANSI_NULLS, QUOTED_IDENTIFIER ON;

-- Step 3: Generate HTML content from the blocking table
DECLARE @HTML NVARCHAR(MAX);
DECLARE @Blocked NVARCHAR(MAX);

DECLARE @ServerName VARCHAR(200),@HasHeadBlocker BIT=0;
select @ServerName = @@SERVERNAME;

DECLARE @Subject NVARCHAR(256);
SET @subject = N'Client ' + @ServerName + ' - Blocking Queries -> Open'; --Change 1



-- Initialize HTML
SET @HTML = '<html>' +
'<head>' +
'<style>' +
'body { font-family: Arial, sans-serif; color: #333; line-height: 1.6; background-color: white; padding: 20px; }' +
'table { width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #ffffff; }' +  
'table, th, td { border: 1px solid #ddd; }' +
'th, td { padding: 12px; text-align: left; }' +
'th { background-color: #008080; color: white; }' +
'tr:nth-child(even) { background-color: #f2f2f2; }' +
'tr:hover { background-color: #3a3f5a; }' +
'p { margin: 0 0 10px; }' +
'</style>' +
'</head>' +
'<body>' +
'<p>Hello Team,</p>' +
'<p>We have detected blocking queries on the <b>' + @ServerName + '</b> server which are currently affecting system performance.</p>' ;

SET @Blocked= '<p>The following outlines the details of the top five blocked sessions:</p>' +
'<table>' +
'<thead>' +
'<tr>' +
'<th>Blocked SPID</th>' +
'<th>Waiting Query Text</th>' +
'<th>Blocking SPID</th>' +
'<th>Blocking Query Text</th>' +
'<th>WaitingInMinutes</th>' +
'<th>WaitType </th>' +
'<th>StartTime</th>' +
'</tr>' +
'</thead>' +
'<tbody>' +
(SELECT TOP (5)
        '<tr>' +
        '<td>' + CAST(b.BlockedSessionID AS NVARCHAR) + '</td>' +
         '<td>' + LEFT(b.QueryWaiting, 300) + '</td>'  +
        '<td>' + CAST(b.BlockingSessionID AS NVARCHAR) + '</td>' +
        '<td>' + LEFT(b.BlockingQuery, 300) + '</td>' +
       '<td>' +CAST( b.WaitingInMinutes AS NVARCHAR) + '</td>' +
       '<td>' +CAST( b.WaitType AS NVARCHAR) + '</td>' +
        '<td>' +CAST( b.StartTime AS NVARCHAR) + '</td>' +       
        '</tr>'
    FROM dbadb..BlockedQueriesInfo b
    WHERE DATEDIFF(SECOND, b.logdate, GETDATE()) < 60 and b.[WaitingInMinutes]>=3
	order by [WaitingInMinutes] desc
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') +
'</tbody>' +
'</table>' 

-- Create the summary of blocking queries
DECLARE @Summary NVARCHAR(MAX);

SELECT @Summary = (
    SELECT 
        '<li>SPID <b>' + CAST(s.head_blocker_session_id AS NVARCHAR) + 
        '</b> is blocking ' + CAST(s.blocking_queries_count AS NVARCHAR) + 
        ' queries and has been running for ' + 
        CONVERT(VARCHAR, DATEADD(SECOND, DATEDIFF(SECOND, MAX(ses.last_request_start_time), GETDATE()), 0), 108) + '.</li>'
     FROM dbadb..HeadBlockingInfo s
    
    INNER JOIN sys.dm_exec_sessions ses ON s.head_blocker_session_id = ses.session_id
	 WHERE DATEDIFF(SECOND, logdate, GETDATE()) < 60
    GROUP BY s.head_blocker_session_id, s.blocking_queries_count
    HAVING DATEDIFF(SECOND, MAX(ses.last_request_start_time), GETDATE()) >= 180
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)');


	select @HasHeadBlocker= case when count(*) >=1 then 1 else 0 end
FROM dbadb..HeadBlockingInfo s
WHERE DATEDIFF(SECOND, s.logdate, GETDATE()) < 60;
if @HasHeadBlocker<>0
BEGIN
SET @HTML = @HTML +'<p><strong>Summary of Impact:</strong></p>' +
'<ul>'+  @Summary +'</ul>' +

'<p>Blocking occurs when one query holds a lock on a resource required by another query, leading to delays and performance degradation.</p>' +
'<p>Below are the details of the blocking sessions identified:</p>' +
'<table>' +
'<thead>' +
'<tr>' +
'<th>Head Blocker SPID</th>' +
'<th>No. Queries Waiting</th>' +
'<th>Blocking Query Text</th>' +
'<th>Host Name</th>' +
'<th>Login Name</th>' +
'<th>Duration</th>' +
'</tr>' +
'</thead>' +
'<tbody>' +
(SELECT 
        '<tr>' +
        '<td>' + CAST(s.head_blocker_session_id AS NVARCHAR) + '</td>' +
        '<td>' + CAST(s.blocking_queries_count AS NVARCHAR) + '</td>' +
        '<td>' + LEFT(s.head_blocker_query, 300) + '</td>' +
        '<td>' + s.host_name + '</td>' +
        '<td>' + s.login_name + '</td>' +
        '<td>' + s.duration + '</td>' +
        '</tr>'
    FROM dbadb..HeadBlockingInfo s
    WHERE DATEDIFF(SECOND, logdate, GETDATE()) < 60
	order by s.duration desc
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') +
'</tbody>' +
'</table>' +'<br>'
END
ELSE
BEGIN
SET @HTML = @HTML  + '</ul>' +

'<p>Blocking occurs when one query holds a lock on a resource required by another query, leading to delays and performance degradation.</p>' +
'<p>Below are the details of the blocking sessions identified:</p>' +
'<table>' +
'<thead>' +
'<tr>' 
END

set @HTML=@HTML +@Blocked + 
 

'<p>The query has been identified as causing significant blocking issues within the database. We recommend promptly reviewing the query to assess whether terminating it is necessary to maintain optimal performance.</p>' +
'<p>Please let us know if you would like us to proceed with terminating these queries or if you’d prefer to allow them to complete their execution.</p>' +
'<p>We are here to assist in resolving this issue.</p>' +
'<p>Best Regards,</p>' +
'<p>MSSQL DBA<br></p>' +  -- change 2
'</body>' +
'</html>';

-- Output the final HTML
--SELECT @HTML AS HTML;







if(@HTML is not null)
BEGIN
EXEC msdb.dbo.Sp_send_dbmail
@profile_name = 'DBmail',     --Change 3 DBmail Profiler
@body = @HTML,
@body_format ='html',
@recipients = '',  -- change 4 Our mail or client mail
@copy_recipients = '',
@blind_copy_recipients='',
@subject =@Subject ;
END

 

GO
SET ANSI_NULLS, QUOTED_IDENTIFIER OFF


--Step 3
SET ANSI_NULLS, QUOTED_IDENTIFIER ON;

-- Step 5: Generate HTML content for closed blocking statuses
DECLARE @HTML_Closed NVARCHAR(MAX), @HTML_Blocked NVARCHAR(MAX)  ,@Summary NVARCHAR(MAX), @ServerName NVARCHAR(128) = @@ServerName,@HasHeadblocker BIT =0;


SET @HTML_Closed = '<html>' +
'<head>' +
'<meta charset="UTF-8">' +
'<meta name="viewport" content="width=device-width, initial-scale=1.0">' +
'<style>' +
'body { font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; background-color: white; padding: 20px; margin: 0; }' +
'header { background: #008080; padding: 10px 0; color: white; text-align: center; }' +
'ul { list-style-type: none; padding: 0; }' +
'ul li { margin: 10px 0; }' +
'table { width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #ffffff; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }' +
'table, th, td { border: 1px solid #ddd; }' +
'th, td { padding: 12px; text-align: left; }' +
'th { background-color: #008000; color: white; }' +
'tr:nth-child(even) { background-color: #f2f2f2; }' +
'tr:hover { background-color: #e0f7fa; }' +
'p { margin: 10px 0; }' +
'footer { margin-top: 20px; font-size: 0.9em; color: #777; text-align: center; }' +
'</style>' +
'</head>' +
'<body>' +
'<p>Hello Team,</p>' +
'<p>This is a notification regarding previously detected blocking queries on the <b>' + @@ServerName + '</b> server that have now been resolved.</p>' +
'<p><strong>Summary of Resolved Blocking Queries:</strong></p>' +
'<ul>';

-- Create the summary of closed blocking queries
SELECT @Summary = (
    SELECT 
        '<li>SPID <b>' + CAST(s.head_blocker_session_id AS NVARCHAR) + 
        '</b> was blocking ' + CAST(s.blocking_queries_count AS NVARCHAR) + 
        ' queries and has resolved successfully.</li>'
    FROM (
        SELECT 
            head_blocker_session_id,
            blocking_queries_count,
            ROW_NUMBER() OVER (PARTITION BY head_blocker_session_id ORDER BY duration DESC) AS rn
        FROM dbadb..HeadBlockingInfo
        WHERE is_closed = 0 AND DATEDIFF(HOUR, logdate, GETDATE()) < 5 -- Focus on open blocking session_id
    ) s
    WHERE s.rn = 1   and NOT EXISTS (SELECT    
    1
    FROM 
    sys.dm_os_waiting_tasks b 
INNER JOIN 
    sys.dm_exec_requests r ON r.session_id = b.session_id and r.session_id>50 
OUTER APPLY 
    sys.dm_exec_sql_text(r.sql_handle) t 
WHERE 
    b.blocking_session_id =s.head_blocker_session_id)
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)');

	SET @HTML_Blocked =  '</ul>' +
'<p>Below are the details of the resolved blocked sessions:</p>' +
'<table>' +
'<thead>' +
'<tr>' +
'<th>Blocked SPID</th>' +
'<th>Waiting Query Text</th>' +
'<th>Blocking SPID</th>' +
'<th>Blocking Query Text</th>' +
'<th>WaitingInMinutes</th>' +
'<th>WaitType </th>' +
'<th>StartTime</th>' +
'</tr>' +
'</thead>' +
'<tbody>' +
(SELECT TOP(5)
        '<tr>' +
        '<td>' + CAST(b.BlockedSessionID AS NVARCHAR) + '</td>' +
         '<td>' + LEFT(b.QueryWaiting, 300) + '</td>'  +
        '<td>' + CAST(b.BlockingSessionID AS NVARCHAR) + '</td>' +
        '<td>' + LEFT(b.BlockingQuery, 300) + '</td>' +
       '<td>' +CAST( max(b.WaitingInMinutes) AS NVARCHAR) + '</td>' +
       '<td>' +CAST( b.WaitType AS NVARCHAR) + '</td>' +
        '<td>' +CAST( b.StartTime AS NVARCHAR) + '</td>' +       
        '</tr>'
    FROM dbadb..BlockedQueriesInfo b
    WHERE DATEDIFF(HOUR, b.logdate, GETDATE()) <= 1 and b.[WaitingInMinutes]>=3 and is_closed=0
	group by BlockedSessionID,LEFT(b.QueryWaiting, 300),b.BlockingSessionID,LEFT(b.BlockingQuery, 300),b.WaitType,b.StartTime
		order by  max(b.WaitingInMinutes) desc
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') +
'</tbody>' +
'</table>' 

 select @HasHeadblocker= case  when count(*)>=1 THEN 1 ELSE 0 END from  dbadb..HeadBlockingInfo   WHERE DATEDIFF(HOUR, logdate, GETDATE()) <= 5 AND is_closed = 0;
  
if @HasHeadblocker<>0
BEGIN
SET @HTML_Closed = @HTML_Closed + @Summary + '</ul>' +
'<p>Below are the details of the resolved blocking sessions:</p>' +
'<table>' +
'<thead>' +
'<tr>' +
'<th>Head Blocker SPID</th>' +
'<th>No. Queries Waiting</th>' +
'<th>Blocking Query Text</th>' +
'<th>Host Name</th>' +
'<th>Login Name</th>' +
'<th>Duration</th>' +
'</tr>' +
'</thead>' +
'<tbody>' +
(SELECT 
        '<tr>' +
        '<td>' + CAST(s.head_blocker_session_id AS NVARCHAR) + '</td>' +
        '<td>' + CAST(s.blocking_queries_count AS NVARCHAR) + '</td>' +
        '<td>' + LEFT(s.head_blocker_query, 300) + '</td>' +
        '<td>' + s.host_name + '</td>' +
        '<td>' + s.login_name + '</td>' +
        '<td>' + CAST(s.duration AS NVARCHAR) + '</td>' + -- Cast duration to NVARCHAR
        '</tr>'
    FROM (
        SELECT 
            head_blocker_session_id,
            blocking_queries_count,
            host_name,
            login_name,
            head_blocker_query,
            duration,
            ROW_NUMBER() OVER (PARTITION BY head_blocker_session_id ORDER BY duration DESC) AS rn
        FROM dbadb..HeadBlockingInfo
        WHERE is_closed = 0 AND DATEDIFF(HOUR, logdate, GETDATE()) < 5 -- Focus on open blocking
    ) s
	WHERE s.rn = 1 and NOT EXISTS (SELECT    
    1
    FROM 
    sys.dm_os_waiting_tasks b 
INNER JOIN 
    sys.dm_exec_requests r ON r.session_id = b.session_id and r.session_id>50 
OUTER APPLY 
    sys.dm_exec_sql_text(r.sql_handle) t 
WHERE 
    b.blocking_session_id =s.head_blocker_session_id)
    
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') +
'</tbody>' +
'</table>' +
'<br>'+ isnull(@HTML_Blocked,'')+
'<p>We appreciate your attention to these matters and are glad to report that performance has returned to normal.</p>' +
'<p>If you have any questions or require further details, please do not hesitate to reach out.</p>' +
'<p>Best Regards,</p>' +
'<p>MSSQL DBA<br></p>' +  --Change 2
'</body>' +
'</html>';

END

ELSE

BEGIN
SET @HTML_Closed = @HTML_Closed +@HTML_Blocked+

'<p>We appreciate your attention to these matters and are glad to report that performance has returned to normal.</p>' +
'<p>If you have any questions or require further details, please do not hesitate to reach out.</p>' +
'<p>Best Regards,</p>' +
'<p>MSSQL DBA<br></p>' +  --Change 2
'</body>' +
'</html>';


END







-- Output the final HTML for closed queries


 UPDATE dbadb..[BlockedQueriesInfo]  SET is_closed = 1

 WHERE DATEDIFF(HOUR, logdate, GETDATE()) <= 1 AND is_closed=0 and NOT EXISTS (SELECT    
    1
    FROM 
    sys.dm_os_waiting_tasks b 
INNER JOIN 
    sys.dm_exec_requests r ON r.session_id = b.session_id and r.session_id>50 
OUTER APPLY 
    sys.dm_exec_sql_text(r.sql_handle) t 
WHERE b.blocking_session_id<>0 and 
    b.session_id=BlockedSessionID );



-- Output the final HTML for closed queries


 UPDATE dbadb..HeadBlockingInfo  SET is_closed = 1

 WHERE DATEDIFF(HOUR, logdate, GETDATE()) <= 5 AND is_closed = 0 and NOT EXISTS (SELECT    
    1
    FROM 
    sys.dm_os_waiting_tasks b 
INNER JOIN 
    sys.dm_exec_requests r ON r.session_id = b.session_id and r.session_id>50 
OUTER APPLY 
    sys.dm_exec_sql_text(r.sql_handle) t 
WHERE 
    b.blocking_session_id =HeadBlockingInfo.head_blocker_session_id);

DECLARE @subject NVARCHAR(256);

--Select @HTML_Closed;
 SET @subject = N'Client ' + @ServerName + ' - Blocking Queries -> Closed'; --Change 1
 IF (@HTML_Closed IS NOT NULL)
BEGIN
EXEC msdb.dbo.sp_send_dbmail
@profile_name = 'DBMAIL',     -- Change 3 DBMail Profiler
@body = @HTML_Closed,
@body_format = 'html',
@recipients = '',
@copy_recipients ='', -- change 4 Client Mail or Our alert mail
 @subject =@subject
END

go
SET ANSI_NULLS, QUOTED_IDENTIFIER OFF;



