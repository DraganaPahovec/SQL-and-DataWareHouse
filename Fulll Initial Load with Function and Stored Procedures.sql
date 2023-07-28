/*1-Create scalar function that will return the total available amount (MKD) for input @NationalNumber. 
Available amount should be calculated as allowed owerdraft + current balance
E.g. my current balance is + 10.000 and I have  40.000 allowed overdraft, which means that my total available amount is 50.000
*/

CREATE OR ALTER FUNCTION dbo.TotalAvailableAmountInMKD (@NationalIDNumber NVARCHAR (15))
RETURNS DECIMAL (18,2)
AS
BEGIN
	DECLARE @Result DECIMAL (18,2)
	SELECT @Result = SUM(a.CurrentBalance + a.AllowedOverdraft)
	FROM dbo.AccountDetails as ad
	INNER JOIN dbo.Account as a ON ad.AccountID = a.ID
	INNER JOIN dbo.Customer as c ON a.CustomerID = c.ID
	INNER JOIN dbo.Currency as cy ON a.CurrencyID = cy.ID
	WHERE c.NationalIDNumber = @NationalIDNumber AND cy.ShortName = 'MKD'
	RETURN @Result
END

--Return the function for all ID numbers
SELECT c.*, dbo.TotalAvailableAmountInMKD (c.NationalIDNumber) as TotalAvailableAmountInMKD
FROM dbo.Customer as c

--Return the function for the given ID number
SELECT dbo.TotalAvailableAmountInMKD (7137597)

/*2-Create table valued function that for input parameter @NationalIDNumber  will return resultset 
with CurrencyName and current balance
*/

CREATE OR ALTER FUNCTION dbo.fn_ResultSetNatIDNum (@NationalIDNumber NVARCHAR (15))
RETURNS @ResultSet TABLE (CurrencyName NVARCHAR (20), CurrentBalance DECIMAL (18,2))
AS
BEGIN
	INSERT INTO @ResultSet (CurrencyName, CurrentBalance)
	SELECT cy.ShortName, SUM(a.CurrentBalance) as CurrentBalance
	FROM dbo.Account as a
		INNER JOIN dbo.Customer as c ON c.ID = a.CustomerID
		INNER JOIN dbo.Currency as cy ON a.CurrencyID = cy.ID
	WHERE c.NationalIDNumber = @NationalIDNumber
	GROUP BY cy.ShortName
RETURN
END
--Selecting for the given ID number
SELECT * 
FROM dbo.fn_ResultSetNatIDNum (7137597)

--check
SELECT c.NationalIDNumber, cy.ShortName, a.CurrentBalance as CurrentBalance
	FROM dbo.Account as a
		INNER JOIN dbo.Customer as c ON c.ID = a.CustomerID
		INNER JOIN dbo.Currency as cy ON a.CurrencyID = cy.ID
WHERE c.NationalIDNumber = 7137597

/*3--Prepare stored procedure that for LocationId and CurrencyID on input, will return:
Biggest 2 transactions for that location and currency (CustomerName, TransactionAmount)
*/


CREATE OR ALTER PROCEDURE dbo.p_BiggestTransactionsPerLocAndCur
(
	@LocationID int
	,@CurrencyID int
)
AS
BEGIN
;WITH MyCTE 
AS
(
	SELECT
	lo.Name, c.FirstName + ' ' + c.LastName as CustomerName, ad.Amount as TransactionAmount,
	DENSE_RANK () OVER (PARTITION BY lo.Name ORDER BY SUM (ad.Amount) DESC) AS DRN
	FROM dbo.Account as a
	INNER JOIN dbo.AccountDetails as ad ON a.ID = ad.AccountID
	INNER JOIN dbo.Customer as c ON a.CustomerID = c.ID
	INNER JOIN dbo.Location as lo ON ad.LocationId = lo.ID
	WHERE @LocationID = lo.ID 
	GROUP BY lo.Name, c.FirstName, c.LastName, ad.Amount
)
SELECT *
FROM MyCTE
WHERE DRN <= 2

;WITH MyCTE2 
AS
(
	SELECT
	cy.ShortName, c.FirstName + ' ' + c.LastName as CustomerName, ad.Amount as TransactionAmount,
	DENSE_RANK () OVER (PARTITION BY cy.ShortName ORDER BY SUM (ad.Amount) DESC) AS DRN2
	FROM dbo.Account as a
	INNER JOIN dbo.AccountDetails as ad ON a.ID = ad.AccountID
	INNER JOIN dbo.Customer as c ON a.CustomerID = c.ID
	INNER JOIN dbo.Currency as cy ON a.CurrencyID = cy.ID
	WHERE @CurrencyID = cy.ID
	GROUP BY cy.ShortName, c.FirstName, c.LastName, ad.Amount
)
SELECT *
FROM MyCTE2
WHERE DRN2 <= 2
END

EXEC dbo.p_BiggestTransactionsPerLocAndCur @LocationID = 13, @CurrencyID = 1
GO

/*Additionally if the location on input belongs to the "Pelagoniski region" then the procedure will additionally 
return the Purpose code and Purpose description information for the 2 biggest transactions
*/

----
/*4-Create procedure that will list all transactions for specific customer in and specific date interval
Input: CustomerId, ValidFrom, ValidTo
Output: CustomerFullName, LocationName,Amount,Currency
*/

CREATE OR ALTER PROCEDURE dbo.p_TransPerCustInDate
(
	@CustomerID int,
	@ValidFrom date,
	@ValidTo date
)
AS
BEGIN
SELECT c.FirstName + ' ' + c.LastName as CustomerName, lo.Name as LocationName, ad.Amount, cy.ShortName as Currency
FROM dbo.AccountDetails as ad
INNER JOIN dbo.Account as a ON a.ID = ad.AccountID
INNER JOIN dbo.Customer as c ON c.ID = a.CustomerID
INNER JOIN dbo.Location as lo ON ad.LocationId = lo.ID
INNER JOIN dbo.Currency as cy ON a.CurrencyID = cy.ID
WHERE @CustomerID = a.CustomerID AND
ad.TransactionDate BETWEEN @ValidFrom and @ValidTo
END

EXEC.dbo.p_TransPerCustInDate @CustomerID = 5, @ValidFrom = '2019-01-01', @ValidTo = '2020-01-31'

/*Extend the procedure to add input parameter @EmployeeId for the employee that generates the report 
for the list of transactions.
For auditing purposes for each execution of the report (procedure) we want to track which Employee executed the query and the input parameters he used during the execution. 
Prepare table for logging this executions
*/

CREATE TABLE dbo.Employee_IdLogs
(
	ID int IDENTITY (1,1) NOT NULL,
	EmployeeID int,
	CustomerID int,
	ValidFrom date,
	ValidTo date,
	LogDate date
CONSTRAINT [PK_Employee_IdLogs] PRIMARY KEY CLUSTERED
	(
		[ID] ASC
	)
)

ALTER PROCEDURE dbo.p_TransPerCustInDate
(
	@EmployeeId int,
	@CustomerID int,
	@ValidFrom date,
	@ValidTo date
)
AS
BEGIN
SELECT c.FirstName + ' ' + c.LastName as CustomerName, lo.Name as LocationName, ad.Amount, cy.ShortName as Currency
FROM dbo.AccountDetails as ad
INNER JOIN dbo.Account as a ON a.ID = ad.AccountID
INNER JOIN dbo.Customer as c ON c.ID = a.CustomerID
INNER JOIN dbo.Location as lo ON ad.LocationId = lo.ID
INNER JOIN dbo.Currency as cy ON a.CurrencyID = cy.ID
WHERE @CustomerID = a.CustomerID AND
ad.TransactionDate BETWEEN @ValidFrom and @ValidTo
INSERT INTO dbo.Employee_IdLogs (EmployeeID, CustomerID, ValidFrom, ValidTo, LogDate)
VALUES (@EmployeeId, @CustomerID, @ValidFrom, @ValidTo, GETDATE())
END

EXEC.dbo.p_TransPerCustInDate @EmployeeId = 1, @CustomerID = 5, @ValidFrom = '2019-01-01', @ValidTo = '2020-01-31'
SELECT * FROM dbo.Employee_IdLogs

/*Prepare new procedure for reading the logged data
Input: ValidFrom, ValidTo
Output: Employee Name, CustomerName, executions count
*/

CREATE PROCEDURE dbo.p_ShowLoggedData
(
	@ValidFrom date,
	@ValidTo date
)
BEGIN
SELECT e.FirstName + ' ' + e.LastName as EmployeeName, c.FirstName + ' ' + c.LastName as CustomerName
FROM dbo.AccountDetails as ad
INNER JOIN dbo.Account as a ON a.ID = ad.AccountID
INNER JOIN dbo.Customer as c ON c.ID = a.CustomerID
INNER JOIN dbo.Employee as e ON e.ID = a.EmployeeID
WHERE ad.TransactionDate BETWEEN @ValidFrom and @ValidTo
INSERT INTO dbo.Employee_IdLogs (ValidFrom, ValidTo, LogDate)
VALUES (@ValidFrom, @ValidTo, GETDATE())
END

EXEC.dbo.p_ShowLoggedData  @ValidFrom = '2019-01-01', @ValidTo = '2020-01-31'

/*H4 - 5-Create procedure that will list Avarage Salary for Customer from his last 3 Paychecks (purpose code 101) on his MKD account,
Input: CustomerId, 
Output: CustomerFullName,AverageSalaryFromLast3Months, AllowedOverdraft
Note: insert new rows if you need more than 3 paychecks in account details
*/
CREATE OR ALTER PROCEDURE dbo.p_Last3AvgSalary
(
	@CustomerId int
)
AS 
BEGIN
	CREATE TABLE #CustLast3MnthsSalary
	(
		CustomerName nvarchar (50)
	,	AllowedOverdraft decimal (18,2)
	,	MonthlySalary decimal (18,2)
	,	RN tinyint
	)

	;WITH CTE_Avg
	AS
	(
		SELECT c.FirstName + ' ' + c.LastName as CustomerName, a.AllowedOverdraft, ad.Amount as Salary
			, ROW_NUMBER () OVER (PARTITION BY a.CustomerID ORDER BY ad.TransactionDate desc) as RN
		FROM dbo.Customer as c
		INNER JOIN dbo.Account as a ON a.CustomerID = c.ID
		INNER JOIN dbo.AccountDetails as ad ON ad.AccountID = a.ID
		WHERE ad.PurposeCode = 101 AND a.CurrencyID = 1 AND CustomerID = @CustomerId
		GROUP BY c.FirstName, c.LastName, a.AllowedOverdraft, ad.Amount, a.CustomerID, ad.TransactionDate
	)
	INSERT INTO #CustLast3MnthsSalary (CustomerName, AllowedOverdraft, MonthlySalary, RN)
	SELECT * FROM CTE_Avg
	WHERE RN <=3

	SELECT CustomerName, AllowedOverdraft, AVG (MonthlySalary) as AverageSalary
	FROM #CustLast3MnthsSalary
	GROUP BY CustomerName, AllowedOverdraft
END
GO

EXEC dbo.p_Last3AvgSalary @CustomerId = 1

DECLARE @CustomerId int = 1

SELECT *
FROM dbo.Account as a
INNER JOIN dbo.AccountDetails as ad ON ad.AccountID = a.ID
WHERE a.CustomerID = @CustomerId AND ad.PurposeCode = 101

/*Create New procedure that will fix the current Customer Allowed Overdraft If it is different than 
the calculated averageSalary * 2 from previous Procedure.
Input: CustomerId
Output – If the customer needed Allowed Overdraft update then update the AllowedOverdraft 
and return Customer Name and flag named NeedsFix with values true or false depending if 
the customer current Allowed Overdraft was different than the new calculated Value 
*/

CREATE OR ALTER PROCEDURE dbo.p_FixAvgSalary
(
	@CustomerID int
)
AS
BEGIN 
	CREATE TABLE #FixAvgSalary
	(
		CustomerID int
	,	CustomerName nvarchar (50)
	,	AllowedOverdraft decimal (18,2)
	,	AverageSalary decimal (18,2)
	)
	INSERT INTO #FixAvgSalary (CustomerID, CustomerName, AllowedOverdraft, AverageSalary)	
	EXEC dbo.p_Last3AvgSalary @CustomerId = @CustomerId

	DECLARE @AllowedOverdraft decimal (18,2)
	SELECT @AllowedOverdraft = @AllowedOverdraft FROM #FixAvgSalary WHERE @CustomerID = @CustomerID

	DECLARE @AverageSalary decimal (18,2)
	SELECT @AverageSalary = @AverageSalary FROM #FixAvgSalary WHERE @CustomerID = @CustomerID

	DECLARE @NeedsFix nvarchar (50)
	IF @AllowedOverdraft <> @AverageSalary * 2
		BEGIN 
			UPDATE dbo.Account 
			SET AllowedOverdraft = @AverageSalary * 2 WHERE @CustomerID = @CustomerID
			SET @NeedsFix = 'TRUE'
		SELECT (SELECT CustomerName FROM #FixAvgSalary), @NeedsFix as NeedsFix
	END
	ELSE 
		BEGIN 
			SET @NeedsFix = 'False'
			SELECT (SELECT CustomerName FROM #FixAvgSalary), @NeedsFix as NeedsFix
		END
END
--Execute the procedure
EXEC dbo.p_FixAvgSalary @CustomerId = 1



















