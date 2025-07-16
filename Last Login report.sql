DECLARE @tableHTML NVARCHAR(MAX)

SET @tableHTML = 
N'<html>
<head>
<style>
    body { font-family: Arial, sans-serif; }
    h2 { color: #2F4F4F; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background-color: #f2f2f2; color: #333; }
    tr:hover { background-color: #f5f5f5; }
</style>
</head>
<body>
    <h2>SQL Server Monthly Login Report</h2>
    <table>
        <tr>
            <th>Login</th>
            <th>Login Type</th>
            <th>Status</th>
            <th>Create Date</th>
            <th>Modify Date</th>
            <th>Last Login Time</th>
        </tr>' +
        CAST((
            SELECT 
                td = sp.name, '',
                td = sp.type_desc, '',
                td = CASE WHEN sp.is_disabled = 1 THEN 'Disabled' ELSE 'Enabled' END, '',
                td = CONVERT(VARCHAR, sp.create_date, 120), '',
                td = CONVERT(VARCHAR, sp.modify_date, 120), '',
                td = ISNULL(CONVERT(VARCHAR, ls.last_login_time, 120), 'Never Logged In Last 30 Days')
            FROM sys.server_principals sp
            LEFT JOIN sys.sql_logins sl ON sp.principal_id = sl.principal_id
            LEFT JOIN [DBADB].[dbo].[LoginLastSeen] ls ON sp.name = ls.server_principal_name
            WHERE sp.type NOT IN ('G', 'R')
            ORDER BY sp.name
            FOR XML PATH('tr'), TYPE
        ) AS NVARCHAR(MAX)) +
    N'</table>
   
</body>
</html>'

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'DBA',
    @recipients = 'sivaraman@geopits.com',
    @subject = 'SQL Server Monthly Login Report',
    @body = @tableHTML,
    @body_format = 'HTML';
