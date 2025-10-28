WITH IndexDetails AS (
    SELECT 
        i.name AS IndexName,
        OBJECT_NAME(i.object_id) AS TableName,
        i.type_desc AS IndexType,
        ic.index_column_id AS ColumnOrder,
        c.name AS ColumnName,
        ic.is_included_column AS IsIncludedColumn,
        ic.is_descending_key AS IsDescending,
        i.fill_factor AS Fill_Factor,
        p.data_compression_desc AS Compression,
        p.partition_number,
        p.rows AS Rows,
        ps.name AS PartitionScheme,
        pc.name AS PartitionColumn
    FROM sys.indexes i
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    LEFT JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
    LEFT JOIN sys.index_columns icp ON i.object_id = icp.object_id AND i.index_id = icp.index_id AND icp.partition_ordinal = 1
    LEFT JOIN sys.columns pc ON icp.object_id = pc.object_id AND icp.column_id = pc.column_id
    WHERE OBJECT_NAME(i.object_id) = 'AttendanceSummary' -- Replace with your table name
)
SELECT 
    IndexName,
    MAX(TableName) AS TableName,
    MAX(IndexType) AS IndexType,
    STUFF((
        SELECT DISTINCT  ', ' + ColumnName + ' ' + CASE WHEN IsDescending = 1 THEN 'DESC' ELSE 'ASC' END
        FROM IndexDetails d2
        WHERE d2.IndexName = d1.IndexName AND d2.IsIncludedColumn = 0
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS KeyColumns,
    STUFF((
        SELECT DISTINCT  ', ' + ColumnName + ' ' + CASE WHEN IsDescending = 1 THEN 'DESC' ELSE 'ASC' END
        FROM IndexDetails d2
        WHERE d2.IndexName = d1.IndexName AND d2.IsIncludedColumn = 1
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS IncludedColumns,
    MAX(Fill_Factor) AS Fill_Factor,
    MAX(Compression) AS Compression,
    MAX(PartitionScheme) AS PartitionScheme,
    MAX(PartitionColumn) AS PartitionColumn,
    SUM(Rows) AS Rows
FROM IndexDetails d1
GROUP BY IndexName;
