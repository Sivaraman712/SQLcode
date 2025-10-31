USE [DBADB]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =======================================================================================================================
-- Author:		Sivaraman
-- Create date: 31/10/2025
-- Description:	This stored procedure identifies and rebuilds indexes in the MARSPROD database Contd from Yesterday
				--that meet the following criteria:
					-- Belong to user-defined tables or views (types 'U' or 'V')
					-- Have non-null index names
					-- Use 'IN_ROW_DATA' allocation
					-- Fragmentation > 30% and page count > 1000, an Index Rebuild will be performed.
					--Fragmentation between 5% and 30% and page count > 1000, an Index Reorganize will be performed.
					--It will rebuild the indexes that were not rebuilt yesterday.
					-- Optional to filter the rebuild process for particular set of tables
					--It is safe to rerun. In case of failure, it will not rebuild the already rebuilt indexes on that day.
-- ========================================================================================================================
CREATE PROCEDURE [dbo].[DBA_Index_Maintenance_Cont]
	
AS
BEGIN
	
	SET NOCOUNT ON;
	IF ((SELECT COUNT(1)  	FROM DBADB..CommandLog    WHERE CAST(StartTime AS DATE) = CAST(GETDATE()-1  AS DATE)
       			AND CommandType = 'ALTER_INDEX'
       			AND EndTime IS NOT NULL)>0 AND (SELECT COUNT(1)  FROM DBADB..CommandLog   WHERE CAST(StartTime AS DATE) = CAST(GETDATE()-2  AS DATE)
       			AND CommandType = 'ALTER_INDEX'
       			AND EndTime IS NOT NULL)=0)
    BEGIN

	DECLARE @IndexList NVARCHAR(MAX);
	SELECT @IndexList = STRING_AGG(
				CAST(QUOTENAME('MARSPROD') AS NVARCHAR(MAX)) + N'.' +
				CAST(QUOTENAME(s.name) AS NVARCHAR(MAX)) + N'.' +
				CAST(QUOTENAME(o.name) AS NVARCHAR(MAX)) + N'.' +
				CAST(QUOTENAME(i.name) AS NVARCHAR(MAX)),
				N','  -- Separator
			)
			WITHIN GROUP (ORDER BY ips.page_count ASC)
			FROM MARSPROD.sys.indexes i
			LEFT JOIN MARSPROD.sys.dm_db_index_physical_stats(DB_ID('MARSPROD'), NULL, NULL, NULL, NULL) ips
				ON ips.object_id = i.object_id AND ips.index_id = i.index_id
			LEFT JOIN MARSPROD.sys.objects o
				ON o.object_id = i.object_id
			LEFT JOIN MARSPROD.sys.schemas s
				ON o.schema_id = s.schema_id
			WHERE ips.alloc_unit_type_desc = 'IN_ROW_DATA'
			  AND i.name IS NOT NULL
			  AND o.type IN ('U', 'V')
			 -- AND o.name in ( select TableName from dbadb..rebuildtablelist ) -- if listed tables only  to rebuild 
			  AND ips.page_count > 1000
			  AND NOT EXISTS (
					SELECT 1
					FROM DBADB..CommandLog cl WITH (NOLOCK)
					WHERE cl.IndexName = i.name
					  AND CAST(StartTime AS DATE) IN (CAST(GETDATE() AS DATE), CAST(GETDATE()-1 AS DATE))
					  AND CommandType = 'ALTER_INDEX'
					  AND EndTime IS NOT NULL
					  AND ErrorMessage IS NULL
				)
			OPTION (RECOMPILE);
		
   
		IF @IndexList is not null AND LEN(@IndexList) > 0
			BEGIN	
			EXECUTE dbadb.dbo.IndexOptimize
			@Databases = 'MARSPROD', 
			@FragmentationLow = NULL,
			@FragmentationMedium = 'INDEX_REORGANIZE,INDEX_REBUILD_OFFLINE',
			@FragmentationHigh = 'INDEX_REBUILD_OFFLINE',
			@FragmentationLevel1 = 5,
			@FragmentationLevel2 = 30,
			@UpdateStatistics = 'ALL',
			@OnlyModifiedStatistics = 'Y',
			@LogToTable = 'Y',
			@Indexes = @IndexList;
		
    END
	ELSE 	PRINT 'No Remaining Index';
    END

    
END
GO


