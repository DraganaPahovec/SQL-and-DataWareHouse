/*Prepare procedures for incremental data load, for the dimension Account and fact AccountDetails in BrainsterDW2 from the first initial load
Integration.InsertDimensionAccount_Incremental
AccountNumber – SCD type1
AccountOverdraft – SCD type2
Integration. InsertFactAccountDetails_Incremental
*/
ALTER TABLE dimension.Account
ADD
	ValidFrom date
,	ValidTo date
,	ModifiedDate datetime
GO

ALTER TABLE dimension.Currency
ADD
	ValidFrom date
,	ValidTo date
,	ModifiedDate datetime
GO

ALTER TABLE dimension.Employee
ADD
	ValidFrom date
,	ValidTo date
,	ModifiedDate datetime
GO

ALTER TABLE dimension.Customer
ADD
	ValidFrom date
,	ValidTo date
,	ModifiedDate datetime
GO

UPDATE 
	dimension.Account
SET	
	ValidFrom = '1753-01-01'
,	ValidTo = '9999-12-31' --selecting a very distanced time frame in order to catch all the changes
GO

SELECT * FROM dimension.Account

--AccountNumber = '310123456789019' - SCD Type 1
--AllowedOverdraft = '1.000.000' - SCD Type 2
--WHERE AccountID = 7

--UPDATE
--	BrainsterDB.dbo.Account
--SET
--	AccountNumber = '310123456789019'
--,	AllowedOverdraft = '1000000'
--WHERE
--	id = 7

SELECT * FROM BrainsterDB.dbo.Account WHERE ID = 7
SELECT * FROM dimension.Account WHERE AccountID = 7

--AccountNumber – SCD type1
--AccountOverdraft – SCD type2
CREATE OR ALTER PROCEDURE Integration.InsertDimensionAccount_Incremental
(
	@Workday date
)
AS
BEGIN
	DECLARE @MaxDate date = '9999-12-31'

CREATE TABLE #AccountChanges
	(
		[AccountID] [int] NOT NULL,
		[AccountNumber] [nvarchar](20) NULL,
		[AllowedOverdraft] [decimal](18,2) NULL,
		[ValidFrom] [date] NULL,
		[ValidTo] [date] NULL,
		[ModifiedDate] [datetime] NULL
		
	)

--SCD Type2 - Update
	insert into #AccountChanges (AccountID, AccountNumber, AllowedOverdraft)
	select
		a.ID, a.AccountNumber, a.AllowedOverdraft
	from
		dimension.Account as da
		inner join BrainsterDB.dbo.Account as a ON da.AccountID = a.ID
	where
		da.ValidFrom <= @Workday and @Workday < da.ValidTo
	and da.ValidTo = @MaxDate

--SCD Type 1 - Just update
	UPDATE
		da
	set
		da.AccountNumber = a.AccountNumber
	,	da.ModifiedDate = GETDATE()
	--select
	--	*
	from
		dimension.Account as da
		inner join BrainsterDB.dbo.Account as a on da.AccountID = a.ID
	where
		da.ValidFrom <= @Workday and @Workday < da.ValidTo
	and da.ValidTo = @MaxDate
	and (
			ISNULL(da.AccountNumber,N'') <> ISNULL(a.AccountNumber,N'')
		)

--SCD Type 2 - Set ValidTo for the Changed data
	update
		da
	set
		da.ValidTo = @Workday
	,	da.ModifiedDate = GETDATE()
	from
		dimension.Account as da
		inner join #AccountChanges as ac on da.AccountID = ac.AccountID
	where
		da.ValidFrom <= @Workday and @Workday < da.ValidTo
	and da.ValidTo = @MaxDate

--SCD Type 2 - insert new values for the Changed data
	insert into dimension.Account([AccountID], [AccountNumber], [AllowedOverdraft], [ValidFrom], [ValidTo], [ModifiedDate])
	select
		ac.[AccountID], ac.[AccountNumber], ac.[AllowedOverdraft] 
	,	@Workday as ValidFrom, @MaxDate as ValidTo, GETDATE() as ModifiedDate
	from
		dimension.Account as da
		inner join #AccountChanges as ac on da.AccountID = ac.AccountID
	where
		da.ValidTo = @Workday

--Insert New rows
	insert into dimension.Account([AccountID], [AccountNumber], [AllowedOverdraft], [ValidFrom], [ValidTo], [ModifiedDate])
	select
		a.ID, a.[AccountNumber], a.[AllowedOverdraft] 
	,	@Workday as ValidFrom, @MaxDate as ValidTo, GETDATE() as ModifiedDate
	from
		[BrainsterDB].dbo.Account as a
	where not exists
	(
		select * from dimension.Account as da where a.ID = da.AccountID and da.ValidTo = @MaxDate
	)
END
GO

EXEC Integration.InsertDimensionAccount_Incremental '2019-04-30'

SELECT * FROM BrainsterDB.dbo.Account WHERE ID = 7
SELECT * FROM dimension.Account WHERE AccountID = 7

--=========================================================================
--Creates Procedure for Incremental load AccountDetails Fact
--=========================================================================
SELECT * FROM BrainsterDB.dbo.AccountDetails
SELECT * FROM fact.AccountDetails
SELECT * FROM dimension.Date

CREATE TABLE integration.LastAggregation
(
	FactName nvarchar (50)
,	LastAggregation date
)
GO

INSERT INTO integration.LastAggregation (FactName, LastAggregation)
SELECT 'fact.AccountDetails' as FactName, MAX (DateKey) as LastAggregation
FROM fact.AccountDetails

SELECT * FROM integration.LastAggregation

CREATE PROCEDURE [integration].[InsertFactAccountDetails_Incremental]
(
	@Workday date
)
AS
BEGIN
	declare @LastAggregation date

	set @LastAggregation = (select top(1) LastAggregation from integration.LastAggregation where FactName = 'fact.AccountDetails')

	CREATE TABLE #AccountDetailsChanges
	(
		[CustomerKey] [int] NOT NULL,
		[CurrencyKey] [int] NOT NULL,
		[EmployeeKey] [int] NOT NULL,
		[AccountKey] [date] NOT NULL,
		[DateKey] [date] NOT NULL,
		[CurrentBalance] [decimal] (18,2) NULL,
		[InflowTransactionsQuantity] [int] NOT NULL,
		[InflowAmount] [decimal](18,2) NOT NULL,
		[OutflowTransactionsQuantity] [int] NOT NULL,
		[OutflowAmount] [decimal](18, 6) NOT NULL,
		[OutflowTransactionsQuantityATM] [int] NOT NULL,
		[OutflowAmountATM] [decimal](18, 6) NOT NULL
	)
	
	;WITH CTE AS
	(
		SELECT
		a.CustomerId, a.CurrencyId, a.EmployeeId, a.ID as AccountID, d.LastDayOfMonth, d.FirstDayOfMonth, ad.TransactionDate
		, ROW_NUMBER() OVER (PARTITION BY a.ID, d.LastDayOfMonth ORDER BY ad.TransactionDate) as RN
		FROM
		BrainsterDB.dbo.Account as a
		INNER JOIN BrainsterDB.dbo.AccountDetails as ad ON a.ID = ad.AccountID
		INNER JOIN dimension.Date as d ON d.DateKey = cast(ad.TransactionDate as date)

		WHERE @LastAggregation < ad.TransactionDate AND ad.TransactionDate <= @Workday
	)
	INSERT INTO #AccountDetailsChanges ([CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM])
	SELECT
		dc.CustomerID, dcu.CurrencyID, de.EmployeeID, da.AccountID, cte.LastDayOfMonth as DateKey 
	,	(
			SELECT
				SUM(amount) 
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
			WHERE
				ad.AccountID = cte.AccountID
			and ad.TransactionDate <= cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
		) as CurrentBalance
	,	(
			SELECT
				COUNT(ad.Amount) 
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
			where
				ad.AccountID = cte.AccountID
			and ad.TransactionDate BETWEEN cte.firstDayOfMonth and cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
			and ad.Amount > 0
		) as InflowTransactionQuantity
	,	(
			SELECT
				ISNULL(SUM(ad.Amount) ,0)
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
			WHERE
				ad.AccountID = cte.AccountID
			and ad.TransactionDate BETWEEN cte.firstDayOfMonth and cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
			and ad.Amount > 0
		) as InflowAmount
	,	(
			SELECT
				COUNT(ad.Amount) 
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
			WHERE
				ad.AccountID = cte.AccountID
			and ad.TransactionDate BETWEEN cte.firstDayOfMonth and cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
			and ad.Amount < 0
		) as OutflowTransactionQuantity
	,	(
			SELECT
				ISNULL(SUM(ad.Amount) ,0)
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
			WHERE
				ad.AccountID = cte.AccountID
			and ad.TransactionDate BETWEEN cte.firstDayOfMonth and cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
			and ad.Amount < 0
		) as OutflowAmount
	,	(
			SELECT
				COUNT(ad.Amount) 
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
				INNER JOIN [BrainsterDB].dbo.[Location] as l on ad.LocationId = l.Id
				INNER JOIN [BrainsterDB].dbo.LocationType as lt on l.LocationTypeId = lt.Id
			WHERE
				ad.AccountID = cte.AccountID
			and ad.TransactionDate BETWEEN cte.firstDayOfMonth and cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
			and ad.Amount < 0
			and lt.[Name] = 'ATM'
		) as OutflowTransactionQuantityATM
	,	(
			SELECT
				ISNULL(SUM(amount) ,0)
			FROM
				[BrainsterDB].dbo.AccountDetails as ad
				INNER JOIN [BrainsterDB].dbo.[Location] as l on ad.LocationId = l.Id
				INNER JOIN [BrainsterDB].dbo.LocationType as lt on l.LocationTypeId = lt.Id
			WHERE
				ad.AccountID = cte.AccountID
			and ad.TransactionDate BETWEEN cte.firstDayOfMonth and cte.LastDayOfMonth
			and ad.TransactionDate <= @Workday
			and ad.Amount < 0
			and lt.[Name] = 'ATM'
		) as OutflowAmountATM
	
	FROM
		CTE
		LEFT OUTER JOIN dimension.Account as da ON cte.AccountID = da.AccountID and da.ValidFrom <= @workday and @workday < da.ValidTo
		LEFT OUTER JOIN dimension.Currency as dcu ON cte.CurrencyID = dcu.CurrencyID and dcu.ValidFrom <= @workday and @workday < dcu.ValidTo
		LEFT OUTER JOIN dimension.Employee as de ON cte.EmployeeID = de.EmployeeID and de.ValidFrom <= @workday and @workday < de.ValidTo
		LEFT OUTER JOIN dimension.Customer as dc ON cte.CustomerID = dc.CustomerID and dc.ValidFrom <= @workday and @workday < dc.ValidTo
	WHERE RN = 1
	ORDER BY cte.AccountID, cte.LastDayOfMonth

	--Delete exisitng rows
	delete
		fad
	from
		fact.AccountDetails as fad
	where
		exists
		(
			select * from #AccountDetailsChanges as adc
			where
				adc.CustomerKey = fad.CustomerKey
			and adc.CurrencyKey = fad.CurrencyKey
			and adc.EmployeeKey = fad.EmployeeKey
			and adc.DateKey = fad.DateKey
		)

	--Insert new rows
	insert into fact.AccountDetails ([CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey]. [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM])
	select
		[CustomerKey], [CurrencyKey], [EmployeeKey], [AccountKey], [DateKey], [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], [OutflowTransactionsQuantityATM], [OutflowAmountATM]
	from
		#AccountDetailsChanges

	update
		l
	set
		LastAggregation = @Workday
	from
		integration.LastAggregation as l
	where
		FactName = 'fact.AccountDetails'	
END
GO

EXEC Integration.InsertFactAccountDetails_Incremental '2019-04-30'