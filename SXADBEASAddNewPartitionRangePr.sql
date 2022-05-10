CREATE PROCEDURE SXADBEASAddNewPartitionRange (@Debug bit = 0, @FGNamePrefix varchar(30) = 'EASAuditYear')
AS
SET NOCOUNT ON
BEGIN
	IF  EXISTS (SELECT * FROM sys.partition_functions WHERE Name = 'AuditingByMonthFN')
	BEGIN
		DECLARE @Today date = GetDate()   -- change to GetDate, we need server date, obsolete MTZ function is not necessary
			, @NextEndOfYearDate datetime
			, @NewLineChar char(2) = Char(13) + Char(10) 
			, @SQL varchar(1000) = ''
			, @PartitionDate date
			, @FilegroupFileDate date
			, @ReturnStatus int

		DECLARE @CurrPartitionMaxRange datetimeoffset(3)

		SELECT @NextEndOfYearDate = DATETIMEFROMPARTS(DATEPART(YEAR,(DATEADD(YEAR, 1, @Today))),12, 31, 0,0,0,0)

		SELECT @CurrPartitionMaxRange = CAST(
						(SELECT TOP 1 CAST(Value AS DATETIME2) 
						FROM sys.partition_range_values
						WHERE function_id = (SELECT function_id 
											 FROM sys.partition_functions
											 WHERE name = 'AuditingByMonthFN')
						ORDER BY boundary_id DESC)
					 AS datetime)

		IF @CurrPartitionMaxRange IS NULL
		BEGIN
			RAISERROR ('Error: Cannot have a NULL value for @CurrPartitionMaxRange.', 16, 1)
			RETURN 
		END

		IF @Debug = 1
		BEGIN
			SELECT @Today as '@Today'
				, @NextEndOfYearDate as '@NextEndOfYearDate'
				, @CurrPartitionMaxRange as '@CurrPartitionMaxRange'
		END

		IF @CurrPartitionMaxRange >= @NextEndOfYearDate
		BEGIN
			RETURN
		END
		ELSE
		BEGIN
			WHILE @CurrPartitionMaxRange < @NextEndOfYearDate
			BEGIN
				DECLARE @FGName sysname
					, @DbName sysname = DB_NAME()
				SELECT @PartitionDate = DATEADD (MONTH, 2, @CurrPartitionMaxRange)
				SELECT @CurrPartitionMaxRange = DATEADD (MONTH, 1, @CurrPartitionMaxRange)

				IF DATEPART(month, @PartitionDate) = 1
					SELECT @FGName = CONCAT(ISNULL(@FGNamePrefix,@DBName), (FORMAT( (DATEADD(week, -1, @PartitionDate)), 'yyyy', 'en-US' )))
				ELSE
					SELECT @FGName = CONCAT(ISNULL(@FGNamePrefix,@DBName), (FORMAT( @PartitionDate, 'yyyy', 'en-US' )))

				IF @debug = 1
				BEGIN
					SELECT @FGName as '@FGName', @PartitionDate as '@PartitionDate', @CurrPartitionMaxRange as '@CurrPartitionMaxRange'
					IF NOT EXISTS (SELECT * FROM sys.filegroups WHERE Name = @FGName)
					BEGIN
						EXEC SXADBEASPartitionMgmtPr @PartitionDate, 1, NULL, NULL, NULL, 'YEARLY', 2, @FGNamePrefix
					END
				END
				ELSE
				BEGIN
					IF NOT EXISTS (SELECT * FROM sys.filegroups WHERE Name = @FGName)
					BEGIN
						EXEC @ReturnStatus = SXADBEASPartitionMgmtPr @PartitionDate, 0, NULL, NULL, NULL, 'YEARLY', 2, @FGNamePrefix
						SET @ReturnStatus = ISNULL(NULLIF (@ReturnStatus,0),@@ERROR)
						IF @ReturnStatus <> 0
						BEGIN
							RAISERROR ('Error: Executing PartMgmt_AddFilegroupAndFilesPr outside the while loop', 16, 1)
							RETURN
						END
					END
				END
				SELECT @SQL = CONCAT ('ALTER PARTITION SCHEME AuditingByMonthScheme NEXT USED [', @FGName, '] ;')
				SELECT @SQL = @SQL + CONCAT ('ALTER PARTITION FUNCTION AuditingByMonthFN() SPLIT RANGE (''', @CurrPartitionMaxRange, ''') ')

				IF @Debug = 1
				BEGIN
					PRINT @SQL
				END
				ELSE
				BEGIN
					EXEC (@SQL)
					IF @@Error <> 0
					BEGIN
						RAISERROR ('Error: Altering Partition scheme and function', 16, 1)
						RETURN
					END
				END
			END
		END
	END
END

GO


