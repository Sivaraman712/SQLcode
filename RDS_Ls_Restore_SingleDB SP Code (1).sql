CREATE PROCEDURE [dbo].[Ls_Restore_SingleDB]
    @dbname VARCHAR(256),
    @s3_path_arn VARCHAR(500),
    @kmsarn VARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @res_db VARCHAR(256);
    SELECT @res_db = target_db_name FROM ls_db_mapping WHERE dbname = @dbname;

    IF @res_db IS NULL
    BEGIN
        RAISERROR('Target database not found for the given dbname.', 16, 1);
        RETURN;
    END

    -- Get current task status and update Ls_tracking
    DROP TABLE IF EXISTS #task_status;
    CREATE TABLE #task_status (
        task_id INT,
        task_type NVARCHAR(200),
        database_name NVARCHAR(128),
        per_complete INT,
        duration INT,
        lifecycle VARCHAR(50),
        task_info NVARCHAR(MAX),
        last_updated DATETIME,
        created_at DATETIME,
        s3_object_arn NVARCHAR(MAX),
        overwrite_s3_backu INT,
        kms NVARCHAR(MAX),
        fpath VARCHAR(MAX),
        overwrite_file INT
    );

    INSERT INTO #task_status
    EXEC msdb.dbo.rds_task_status @db_name = @res_db;

    WITH CTE AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY s3_object_arn ORDER BY task_id DESC) AS RowNumber
        FROM #task_status
    )
    DELETE FROM CTE WHERE RowNumber > 1;

    UPDATE lt
    SET rstatus = ts.lifecycle
    FROM Ls_tracking lt
    JOIN #task_status ts ON ts.s3_object_arn = lt.arn
    WHERE ISNULL(lt.rstatus, '') <> 'SUCCESS';

    -- Determine the last successful restore epoch
    DECLARE @last_success_epoch BIGINT;
    SELECT @last_success_epoch = MAX(backup_file_epoch)
    FROM Ls_tracking
    WHERE dbname = @dbname AND rstatus = 'SUCCESS';

    -- Recreate failed tasks after last success (if attempts < 5)
    DECLARE @file_epoch BIGINT, @arn VARCHAR(MAX);

    DECLARE failed_cursor CURSOR FOR
    SELECT backup_file_epoch, arn
    FROM Ls_tracking
    WHERE dbname = @dbname
      AND backup_file_epoch > ISNULL(@last_success_epoch, 0)
      AND rstatus IN ('ERROR', 'CANCELLED')
      AND ISNULL(attempt, 0) < 5
    ORDER BY backup_file_epoch;

    OPEN failed_cursor;
    FETCH NEXT FROM failed_cursor INTO @file_epoch, @arn;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC msdb.dbo.rds_restore_log
            @restore_db_name = @res_db,
            @s3_arn_to_restore_from = LOWER(@arn),
            @kms_master_key_arn = @kmsarn,
            @with_norecovery = 1;

        UPDATE Ls_tracking
        SET attempt = ISNULL(attempt, 0) + 1,
            rstatus = 'CREATED'
        WHERE backup_file_epoch = @file_epoch AND dbname = @dbname;

        FETCH NEXT FROM failed_cursor INTO @file_epoch, @arn;
    END

    CLOSE failed_cursor;
    DEALLOCATE failed_cursor;

    -- If any failed tasks still exist after last success, skip new task creation
    IF EXISTS (
        SELECT 1
        FROM Ls_tracking
        WHERE dbname = @dbname
          AND backup_file_epoch > ISNULL(@last_success_epoch, 0)
          AND rstatus IN ('ERROR', 'CANCELLED')
    )
    BEGIN
        RETURN;
    END

    -- Update last restored sequence
    IF @last_success_epoch IS NOT NULL
    BEGIN
        UPDATE ls_db_mapping
        SET Last_restored_seq = @last_success_epoch
        WHERE dbname = @dbname;
    END

    -- Insert new restore tasks with rstatus = 'NOTCREATED'
    INSERT INTO Ls_tracking (
        dbname, dbid, family_guid, backup_seq, backup_file_epoch,
        arn, rstatus, attempt
    )
    SELECT dbname, dbid, family_guid, backup_seq, backup_file_epoch,
           @s3_path_arn + CAST(dbid AS VARCHAR(19)) + '.' + family_guid + '/' +
           CAST(dbid AS VARCHAR(19)) + '.' + family_guid + '.' +
           CAST(backup_seq AS VARCHAR(19)) + '.' + CAST(backup_file_epoch AS VARCHAR(50)) AS arn,
           'NOTCREATED', 0
    FROM LinkedtoRDS.LsMonitor.dbo.tlogbackuplist
    WHERE dbname = @dbname
      AND backup_file_epoch > ISNULL(@last_success_epoch, 0)
      AND backup_file_epoch NOT IN (
          SELECT backup_file_epoch FROM Ls_tracking WHERE dbname = @dbname
      )
    ORDER BY backup_file_epoch;

    -- Restore logs for entries marked as 'NOTCREATED'
    DECLARE restore_cursor CURSOR FOR
    SELECT backup_file_epoch, arn
    FROM Ls_tracking
    WHERE dbname = @dbname
      AND rstatus = 'NOTCREATED'
    ORDER BY backup_file_epoch;

    OPEN restore_cursor;
    FETCH NEXT FROM restore_cursor INTO @file_epoch, @arn;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC msdb.dbo.rds_restore_log
            @restore_db_name = @res_db,
            @s3_arn_to_restore_from = LOWER(@arn),
            @kms_master_key_arn = @kmsarn,
            @with_norecovery = 1;

        UPDATE Ls_tracking
        SET rstatus = 'CREATED',
            attempt = ISNULL(attempt, 0) + 1
        WHERE backup_file_epoch = @file_epoch AND dbname = @dbname;

        FETCH NEXT FROM restore_cursor INTO @file_epoch, @arn;
    END

    CLOSE restore_cursor;
    DEALLOCATE restore_cursor;
END
GO
