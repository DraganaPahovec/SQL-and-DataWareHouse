/*Design BrainsterDW2 database
Design data warehouse for the BrainsterDB
Main purpose of the DW is to keep the accounts balance on monthly level
For each account on monthly level we should store:
-Current balance
-Inflow amount and number of transactions on monthly level
-Outflow amount and number of transactions on monthly level
-Outflow amount ant number of transactions for the ATM transactions only
*/

/* Possibility to filter the data:
By Customer
By Currency
By Employee
By Account
By Date (monthly, quarterly, half year, year)
*/

/*Task1 -Create new database
Create new database schemas (integration, dimension, fact)
*/

CREATE DATABASE BrainsterDW2
GO

USE BrainsterDW2
GO

CREATE SCHEMA dimension
GO

CREATE SCHEMA fact
GO

CREATE SCHEMA integration
GO

/*Task2- Create the dimension tables
Currency 
Employee
Customer
Account
Date (search for some examples for data dimension on internet)
*/

CREATE TABLE dimension.Currency
(
	  CurrencyKey int IDENTITY (1,1) NOT NULL
	, CurrencyID int NOT NULL
	, Code nvarchar (100) NULL
	, Name nvarchar (100) NULL
	, ShortName nvarchar (20) NULL
	, CountryName nvarchar (100) NULL
CONSTRAINT PK_Currency PRIMARY KEY CLUSTERED (CurrencyKey)
)
GO

CREATE TABLE dimension.Employee
(
	  EmployeeKey int IDENTITY (1,1) NOT NULL
	, EmployeeID int NOT NULL
	, FirstName nvarchar (100) NOT NULL
	, LastName nvarchar (100) NOT NULL
	, NationalIDNumber nvarchar (50) NULL
	, JobTitle nvarchar (50) NULL
	, DateOfBirth date NULL
	, MaritalStatus nchar (1) NULL
	, Gender nchar (1) NULL
	, HireDate date NULL
	, CityName nvarchar (50) NOT NULL
	, Region nvarchar (50) NOT NULL
	, [Population] int NULL
CONSTRAINT PK_Employee PRIMARY KEY CLUSTERED (EmployeeKey)
)
GO

CREATE TABLE dimension.Customer
(
	  CustomerKey int IDENTITY (1,1) NOT NULL
	, CustomerID int NOT NULL
	, FirstName nvarchar (100) NOT NULL
	, LastName nvarchar (100) NOT NULL
	, Gender nchar (1) NULL
	, NationalIDNumber nvarchar (15) NULL
	, DateOfBirth date NULL
	, RegionName nvarchar (100) NULL
	, PhoneNumber nvarchar (200) NULL
	, IsActive bit NOT NULL
	, CityName nvarchar (50) NOT NULL
	, Region nvarchar (50) NOT NULL
	, [Population] int NULL
CONSTRAINT PR_Customer PRIMARY KEY CLUSTERED (CustomerKey)
)
GO

CREATE TABLE dimension.Account
(
	  AccountKey int IDENTITY (1,1) NOT NULL
	, AccountID int NOT NULL
	, AccountNumber nvarchar (20) NULL
	, AllowedOverdraft decimal (18,2) NULL
CONSTRAINT PK_Account PRIMARY KEY CLUSTERED (AccountKey)
)
GO

CREATE TABLE [dimension].[Date]
(
	[DateKey] [date] NOT NULL,
	[Day] [tinyint] NOT NULL,
	[DaySuffix] [char](2) NOT NULL,
	[Weekday] [tinyint] NOT NULL,
	[WeekDayName] [varchar](10) NOT NULL,
	[IsWeekend] [bit] NOT NULL,
	[IsHoliday] [bit] NOT NULL,
	[HolidayText] [varchar](64) SPARSE  NULL,
	[DOWInMonth] [tinyint] NOT NULL,
	[DayOfYear] [smallint] NOT NULL,
	[WeekOfMonth] [tinyint] NOT NULL,
	[WeekOfYear] [tinyint] NOT NULL,
	[ISOWeekOfYear] [tinyint] NOT NULL,
	[Month] [tinyint] NOT NULL,
	[MonthName] [varchar](10) NOT NULL,
	[Quarter] [tinyint] NOT NULL,
	[QuarterName] [varchar](6) NOT NULL,
	[Year] [int] NOT NULL,
	[MMYYYY] [char](6) NOT NULL,
	[MonthYear] [char](7) NOT NULL,
	[FirstDayOfMonth] [date] NOT NULL,
	[LastDayOfMonth] [date] NOT NULL,
	[FirstDayOfQuarter] [date] NOT NULL,
	[LastDayOfQuarter] [date] NOT NULL,
	[FirstDayOfYear] [date] NOT NULL,
	[LastDayOfYear] [date] NOT NULL,
	[FirstDayOfNextMonth] [date] NOT NULL,
	[FirstDayOfNextYear] [date] NOT NULL,
 CONSTRAINT [PK_Date] PRIMARY KEY CLUSTERED 
(
	[DateKey] ASC
))
GO


/*Task2- Create fact table
AccountDetails
*/
CREATE TABLE fact.AccountDetails
(
	  AccountDetailsKey int IDENTITY (1,1) NOT NULL
	, CustomerKey int NOT NULL
	, CurrencyKey int NOT NULL
	, EmployeeKey int NOT NULL
	, AccountKey int NOT NULL
	, DateKey date NOT NULL
	, CurrentBalance decimal (18,2) NULL
	, InflowTransactionsQuantity int NOT NULL
	, InflowAmount int NOT NULL
	, OutflowTransactionsQuantity int NOT NULL
	, OutflowAmount int NOT NULL
	, OutflowTransactionsQuantityATM int NOT NULL
	, OutflowAmountATM int NOT NULL
CONSTRAINT PK_AccountDetails PRIMARY KEY CLUSTERED (AccountDetailsKey)
)
GO

--Task3- Add foreign keys
ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Currency_AccountDetails FOREIGN KEY (CurrencyKey)
REFERENCES dimension.Currency (CurrencyKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Employee_AccountDetails FOREIGN KEY (EmployeeKey)
REFERENCES dimension.Employee (EmployeeKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Customer_AccountDetails FOREIGN KEY (CustomerKey)
REFERENCES dimension.Customer (CustomerKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Account_AccountDetails FOREIGN KEY (AccountKey)
REFERENCES dimension.Account (AccountKey)
GO

ALTER TABLE fact.AccountDetails
ADD CONSTRAINT FK_Date_AccountDetails FOREIGN KEY (DateKey)
REFERENCES dimension.Date (DateKey)
GO


/*Task4-Prepare procedures for initial data load
One procedure for each dimension
One procedure for the fact table
*/

--Currency dimension procedure
CREATE OR ALTER PROCEDURE integration.InsertDimensionCurrency
AS
BEGIN
INSERT INTO dimension.Currency (CurrencyID, Code, [Name], ShortName, CountryName)
SELECT 
	cy.ID as CurrencyID, cy.Code, cy.Name, cy.ShortName, cy.CountryName
FROM BrainsterDB.dbo.Currency as cy
ORDER BY cy.ID
END
GO

TRUNCATE TABLE dimension.Customer
GO
EXEC [integration].[InsertDimensionCurrency] 
GO
--SELECT * FROM dimension.Currency

EXEC integration.InsertDimensionCurrency

--Customer dimension procedure
CREATE OR ALTER PROCEDURE integration.InsertDimensionCustomer
AS
BEGIN
INSERT INTO dimension.Customer (CustomerID, FirstName, LastName, Gender, NationalIDNumber, DateOfBirth, RegionName, 
PhoneNumber, IsActive, CityName, Region, [Population])
SELECT
	c.ID as CustomerID, c.FirstName, c.LastName, c.Gender, c.NationalIDNumber, c.DateOfBirth, c.RegionName, 
	c.PhoneNumber, c.isActive, ci.Name as CityName, ci.Region, ci.[Population]
FROM BrainsterDB.dbo.Customer as c
	LEFT OUTER JOIN BrainsterDB.dbo.City as ci ON c.CityID = ci.ID
ORDER BY c.ID
END
GO

SELECT * FROM dimension.Customer

EXEC integration.InsertDimensionCustomer
SELECT * FROM dimension.Customer

--Employee dimension procedure

CREATE OR ALTER PROCEDURE integration.InsertDimensionEmployee
AS
BEGIN
INSERT INTO dimension.Employee (EmployeeID, FirstName, LastName, NationalIDNumber, JobTitle, DateOfBirth, MaritalStatus, Gender, HireDate, 
CityName, Region, Population)
SELECT
	e.ID as EmployeeID, FirstName, LastName, NationalIDNumber, JobTitle, DateOfBirth, MaritalSTatus, Gender, HireDate,
	ci.Name as CityName, ci.Region, ci.Population
FROM BrainsterDB.dbo.Employee as e
INNER JOIN BrainsterDB.dbo.City as ci ON e.CityID = ci.ID
ORDER BY e.ID
END
GO

EXEC integration.InsertDimensionEmployee

--Account dimension procedure
CREATE OR ALTER PROCEDURE integration.InsertDimensionAccount
	AS
	BEGIN
	INSERT INTO 
		dimension.Account (AccountID, AccountNumber, AllowedOverdraft)
	SELECT
		a.ID as AccountID, AccountNumber, AllowedOverdraft
	FROM BrainsterDB.dbo.Account as a
	ORDER BY a.ID
	END
GO

EXEC integration.InsertDimensionAccount

--Date dimension procedure
CREATE OR ALTER PROCEDURE [integration].[GenerateDimensionDate]
	AS
	BEGIN
	INSERT INTO dimension.[Date]
	SELECT *  
	FROM BrainsterDW.dimension.Date
	END
GO
SELECT * from dimension.Date
EXEC integration.[GenerateDimensionDate]
SELECT * from dimension.Date

TRUNCATE TABLE dimension.Date --cannot be done, because there are foreign keys active. Instead, can be used DELETE TABLE

--AccountDetails Fact procedure

CREATE PROCEDURE integration.InsertFactAccountDetails
AS
BEGIN
;WITH CTE AS
(
SELECT
a.CustomerId, a.CurrencyId, a.EmployeeId, a.AccountNumber, a.AllowedOverdraft
, d.FirstDayOfMonth, d.LastDayOfMonth
, ad.TransactionDate, a.ID as AccountID
--, ROW_NUMBER() OVER (PARTITION BY a.ID, YEAR(ad.TransactionDate), MONTH(ad.TransactionDate) ORDER BY ad.TransactionDate) as RN
, ROW_NUMBER() OVER (PARTITION BY ad.ID, d.LastDayOfMonth ORDER BY ad.TransactionDate) as RN
FROM
BrainsterDB.dbo.AccountDetails as ad
INNER JOIN BrainsterDB.dbo.Account as a ON a.ID = ad.AccountID
INNER JOIN dimension.Date as d ON d.DateKey = cast(ad.TransactionDate as date)
--WHERE
-- a.Id = 1
)
INSERT INTO fact.AccountDetails([CustomerKey], [CurrencyKey], [EmployeeKey], [DateKey], [AccountKey], 
 [CurrentBalance], [InflowTransactionsQuantity], [InflowAmount], [OutflowTransactionsQuantity], [OutflowAmount], 
 [OutflowTransactionsQuantityATM], [OutflowAmountATM])
SELECT
dc.CustomerKey, dcu.CurrencyKey, de.EmployeeKey, cte.LastDayOfMonth as DateKey, da.AccountKey
, (
SELECT
SUM(ad.Amount)
FROM
BrainsterDB.dbo.AccountDetails as ad
WHERE
ad.AccountId = cte.AccountID
and ad.TransactionDate <= cte.LastDayOfMonth
) as CurrentBalance
, (
SELECT
COUNT(ad.Amount)
FROM
BrainsterDB.dbo.AccountDetails as ad
WHERE
ad.AccountId = cte.AccountID
and ad.Amount > 0
and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
) as InflowTransactionQuantity
, (
SELECT
ISNULL(SUM(ad.Amount),0)
FROM
BrainsterDB.dbo.AccountDetails as ad
WHERE
ad.AccountId = cte.AccountID
and ad.Amount > 0
and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
) as InflowAmount
, (
SELECT
COUNT(ad.Amount)
FROM
BrainsterDB.dbo.AccountDetails as ad
WHERE
ad.AccountId = cte.AccountID
and ad.Amount < 0
and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
) as OutflowTransactionQuantity
, (
SELECT
ISNULL(SUM(ad.Amount),0)
FROM
BrainsterDB.dbo.AccountDetails as ad
WHERE
ad.AccountId = cte.AccountID
and ad.Amount < 0
and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
) as OutflowAmount
, (
SELECT
COUNT(Amount)
FROM
BrainsterDB.dbo.AccountDetails as ad
INNER JOIN BrainsterDB.dbo.[Location] as l ON ad.LocationId = l.Id
INNER JOIN BrainsterDB.dbo.LocationType as lt ON l.LocationTypeId = lt.Id
WHERE
ad.AccountId = cte.AccountID
and ad.Amount < 0
and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
and lt.[Name] = 'ATM'
) as OutflowTransactionQuantityATM
, (
SELECT
ISNULL(SUM(ad.Amount),0)
FROM
BrainsterDB.dbo.AccountDetails as ad
INNER JOIN BrainsterDB.dbo.[Location] as l ON ad.LocationId = l.Id
INNER JOIN BrainsterDB.dbo.LocationType as lt ON l.LocationTypeId = lt.Id
WHERE
ad.AccountId = cte.AccountID
and ad.Amount < 0
and ad.TransactionDate BETWEEN cte.FirstDayOfMonth and cte.LastDayOfMonth
and lt.[Name] = 'ATM'
) as OutflowAmountATM
--...
--, cte.TransactionDate, cte.AccountID
--, cte.FirstDayOfMonth, cte.LastDayOfMonth
FROM
CTE
LEFT OUTER JOIN dimension.Customer as dc ON cte.CustomerId = dc.CustomerID
LEFT OUTER JOIN dimension.Currency as dcu ON cte.CurrencyId = dcu.CurrencyID
LEFT OUTER JOIN dimension.Employee as de ON cte.EmployeeId = de.EmployeeID
LEFT OUTER JOIN dimension.Account as da ON CTE.AccountID = da.AccountID --dopolnitelna dimenzija
WHERE
RN = 1
ORDER BY
cte.AccountID, cte.LastDayOfMonth
END
GO

EXEC.integration.InsertFactAccountDetails

SELECT * FROM fact.AccountDetails
WHERE CustomerKey=1 AND CurrencyKey=1