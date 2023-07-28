/*Extend the ETL workflow - WWI to WWIDW
Create a new procedure for synchronizing the dimension StockItem, in a similar way as we created a procedure for synchronizing the dimension City
[integration] .[LoadDimensionStockItem] (@TargetETLCutOffTime)
*/
--====================================================================================
--LoadDimensionStockItems
--====================================================================================
CREATE PROCEDURE [Integration].[LoadDimensionStockItem]
(
	@TargetETLCutoffTime datetime2(7)
)
AS
BEGIN 
	declare
		@TableName sysname--(nvarchar(128)
	,	@LineageKey int
	,	@LastETLCutoffTime datetime2(7)
	
	declare @Lineage table (LineageKey int);
	declare @ETLCutoff table(CutoffTime datetime2(7));

	--Step 1:
	set @TableName = 'StockItem'
		
	--Step 2:
	INSERT INTO @Lineage (LineageKey)
	EXEC Integration.GetLineageKey @TableName, @TargetETLCutoffTime;
	set @LineageKey = (select top (1) LineageKey from @Lineage)
		
	--Step 3:
	DELETE FROM Integration.StockItem_Staging;
		
	--Step 4
	insert into @ETLCutoff (CutoffTime)
	EXEC Integration.GetLastETLCutoffTime @TableName;
	set @LastETLCutoffTime = (select top(1) CutoffTime from @ETLCutoff)
		
	--Step 5:
	declare @StockItem_Staging table
	(
		[WWI Stock Item ID] [int] NOT NULL,
		[Stock Item] [nvarchar](100) NOT NULL,
		[Color] [nvarchar](20) NOT NULL,
		[Selling Package] [nvarchar](50) NOT NULL,
		[Buying Package] [nvarchar](50) NOT NULL,
		[Brand] [nvarchar](50) NOT NULL,
		[Size] [nvarchar](20) NOT NULL,
		[Lead Time Days] [int] NOT NULL,
		[Quantity Per Outer] [int] NULL,
		[Is Chiller Stock] [bit] NOT NULL,
		[Barcode] [nvarchar](50) NOT NULL,
		[Tax Rate] [decimal](18,3) NOT NULL,
		[Unit Price] decimal (18,2) NOT NULL,
		[Recommended Retail Price] [decimal] (18,2) NULL,
		[Typical Weight Per Unit] [decimal] (18,3) NOT NULL,
		[Photo] [varbinary](max) NULL,
		[Valid From] [datetime2] (7) NOT NULL,
		[Valid To] [datetime2](7) NOT NULL
	)
	INSERT INTO @StockItem_Staging ([WWI Stock Item ID], [Stock Item], [Color], [Selling Package], [Buying Package], [Brand], [Size], [Lead Time Days], [Quantity Per Outer], [Is Chiller Stock], [Barcode], [Tax Rate], [Unit Price], [Recommended Retail Price], [Typical Weight Per Unit], [Photo], [Valid From], [Valid To])
	EXEC WideWorldImporters.Integration.GetStockItemUpdates @LastETLCutoffTime, @TargetETLCutoffTime

	--Step6:
	INSERT INTO Integration.StockItem_Staging ([WWI Stock Item ID], [Stock Item], [Color], [Selling Package], [Buying Package], [Brand], [Size], [Lead Time Days], [Quantity Per Outer], [Is Chiller Stock], [Barcode], [Tax Rate], [Unit Price], [Recommended Retail Price], [Typical Weight Per Unit], [Photo], [Valid From], [Valid To])
	select * from @StockItem_Staging

	--Step 7:
	EXEC Integration.MigrateStagedStockItemData;
END
GO

/*Create a procedure for synchronizing the fact Order, in a similar way as we created a procedure for synchronizing the fact Purchase.
[integration] .[LoadFactOrder] (@TargetETLCutOffTime)*/
CREATE PROCEDURE [Integration].[LoadFactOrder]
(
	@TargetETLCutoffTime datetime2(7)
)
AS
BEGIN
	declare
		@TableName sysname--(nvarchar(128)
	,	@LineageKey int
	,	@LastETLCutoffTime datetime2(7)
	
	declare @Lineage table (LineageKey int);
	declare @ETLCutoff table(CutoffTime datetime2(7));

	--Step 1:
	set @TableName = 'Order'
		
	--Step 2:
	INSERT INTO @Lineage (LineageKey)
	EXEC Integration.GetLineageKey @TableName, @TargetETLCutoffTime;
	set @LineageKey = (select top (1) LineageKey from @Lineage)
		
	--Step 3:
	DELETE FROM Integration.Order_Staging;

	--Step 4
	insert into @ETLCutoff (CutoffTime)
	EXEC Integration.GetLastETLCutoffTime @TableName;
	set @LastETLCutoffTime = (select top(1) CutoffTime from @ETLCutoff)
		
	--Step 5:
	declare @Order_Staging table
	(
		[Order Date Key] date NULL
	,	[WWI Order ID] int NULL
	,	[WWI Backorder ID] int NULL
	,	[Description] nvarchar (100) NULL
	,	[Package] nvarchar (50) NULL
	,	[Quantity] int NULL
	,	[Unit Price] decimal (18,2) NULL
	,	[Tax Rate] decimal (18,3) NULL
	,	[Total Excluding Tax] decimal (18,2) NULL
	,	[Tax Amount] decimal (18,2) NULL
	,	[Total Including Tax] decimal (18,2) NULL
	,	[Lineage Key] int NULL
	,	[WWI City ID] int NULL
	,	[WWI Customer ID] int NULL
	,	[WWI Stock Item ID] int NULL
	,	[WWI Salesperson ID] int NULL
	,	[WWI Picker ID] int NULL
	,	[Last Modified When] datetime2 (7) NULL
	)
	INSERT INTO @Order_Staging ([Order Date Key], [WWI Order ID], [WWI Backorder ID], [Description], [Package], [Quantity], [Unit Price], [Tax Rate]
	,[Total Excluding Tax],	[Tax Amount], [Total Including Tax], [Lineage Key],	[WWI City ID], [WWI Customer ID]
	,[WWI Stock Item ID], [WWI Salesperson ID],	[WWI Picker ID], [Last Modified When])
	EXEC WideWorldImporters.Integration.GetOrderUpdates @LastETLCutoffTime, @TargetETLCutoffTime

	--Step6:
	INSERT INTO Integration.Order_Staging ([Order Date Key], [WWI Order ID],	[WWI Backorder ID],	[Description],	[Package], [Quantity], [Unit Price], [Tax Rate]
	,[Total Excluding Tax],	[Tax Amount], [Total Including Tax], [Lineage Key],	[WWI City ID], [WWI Customer ID]
	,[WWI Stock Item ID], [WWI Salesperson ID],	[WWI Picker ID], [Last Modified When])
	select * from @Order_Staging

	--Step 7:
	EXEC Integration.MigrateStagedOrderData;
END
GO

/*Extend the orchestrator procedure with new procedures and test the ETL process
[integration].[LoadDailyETLMain]
*/
CREATE PROCEDURE [Integration].[LoadDailyETLMain]
AS
BEGIN 
	declare
		@TargetETLCutoffTime datetime2(7)
	,	@YearNumber int

	--Step 1:
	set @TargetETLCutoffTime = DATEADD(Minute, -5, GETUTCDATE())

	--Step 2:
	set @TargetETLCutoffTime = DATEADD(nanosecond, 0 - DATEPART(nanosecond, @TargetETLCutoffTime), @TargetETLCutoffTime)

	--Step 3:
	set @YearNumber =  YEAR(SYSUTCDATETIME());
	EXEC Integration.PopulateDateDimensionForYear @YearNumber;

	--Load Dimension StockItem
	EXEC [Integration].[LoadDimensionStockItem] @TargetETLCutoffTime;

	--Load Fact Order
	EXEC [Integration].[LoadFactOrder] @TargetETLCutoffTime;
END
GO

