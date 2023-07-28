CREATE DATABASE [ProjectHR]
GO

USE [ProjectHR]
GO

CREATE TABLE [Location]
(
	ID int IDENTITY(1,1) not null
,	CountryName nvarchar(100) not null
,	Continent nvarchar(100) not null
,	Region nvarchar(100) not null
,	CONSTRAINT PK_Loacation PRIMARY KEY CLUSTERED (ID ASC)
)
GO

CREATE TABLE [SeniorityLevel]
(
	ID int IDENTITY(1,1) not null
,	[Name] nvarchar(100) not null
,	CONSTRAINT PK_SeniorityLevel PRIMARY KEY CLUSTERED (ID ASC)
)
GO

CREATE TABLE [Department]
(
	ID int IDENTITY(1,1) not null
,	[Name] nvarchar(100) not null
,	CONSTRAINT PK_Department PRIMARY KEY CLUSTERED (ID ASC)
)
GO

CREATE TABLE [Employee]
(
	ID int IDENTITY(1,1) not null
,	FirstName nvarchar(100) not null
,	LastName nvarchar(100) not null
,	LocationID int not null
,	SeniorityLevelID int not null
,	DepartmentID int not null
,	CONSTRAINT PK_Employee PRIMARY KEY CLUSTERED (ID ASC)
)
GO

CREATE TABLE [Salary]
(
	ID int IDENTITY(1,1) not null
,	EmployeeID int not null
,	Month smallint not null
,	Year smallint not null
,	GrossAmount decimal(18,2) not null
,	NetAmount decimal(18,2) not null
,	RegularWorkAmount decimal(18,2) not null
,	BonusAmount decimal(18,2) not null
,	OvertimeAmount decimal(18,2) not null
,	VacationDays smallint not null
,	SickLeaveDays smallint not null
,	CONSTRAINT PK_Salary PRIMARY KEY CLUSTERED (ID ASC)
)
GO

-- Seniority levels
truncate table dbo.SeniorityLevel
GO
insert into dbo.SeniorityLevel (Name) values ('Junior'),('Intermediate'),('Senor'),('Lead'),('Project Manager'),
('Division Manager'),('Office manager'),('CEO'),('CTO'),('CIO')
GO

-- Location
TRUNCATE TABLE dbo.Location
GO
INSERT INTO dbo.location (CountryName,Continent,Region)
select CountryName,Continent,Region
from [WideWorldImporters].[Application].Countries
GO

-- Departments
-- reference site: https://www.quora.com/What-are-the-various-departments-of-a-bank-and-what-are-their-functions
TRUNCATE TABLE dbo.Department
GO
insert into dbo.Department (Name) 
values 
('Personal Banking & Operations'),
('Digital Banking Department'),
('Retail Banking & Marketing Department'),
('Wealth Management & Third Party Products'),
('International Banking Division & DFB'),
('Treasury'),
('Information Technology'),
('Corporate Communications'),
('Support Services & Branch Expansion'),
('Human Resources')
GO

-- EMPLOYEE
delete from dbo.Employee
GO
INSERT INTO dbo.Employee(FirstName,LastName,LocationId,SeniorityLevelId,DepartmentId)
select substring(FullName,1,charindex(N' ',FullName,1)) as FirstName,
substring(FullName,charindex(N' ',FullName,1) + 1, len(FullName)) as LastName,
1,1,1 -- initially set all locations,seniorities,department to 1, and update data after insert
from WideWorldImporters.Application.People where PersonID > 1
GO

-- separate the employees in approx equal number of employees by senioriy,city,department
;WITH CTE AS
(
select *,
	NTILE(10) OVER (ORDER BY ID) as MySeniorityLevel,
	NTILE(190) OVER (ORDER BY FirstName) as MyLocation,
	NTILE(10) OVER (ORDER BY LastName) as MyDepartment
from dbo.employee e
)
update c set LocationId = MyLocation, 
	SeniorityLevelId = MySeniorityLevel,
	DepartmentId = MyDepartment	
from cte c
GO


-- SALARY generator
drop table if exists #rn;
;with rn as
(
	select top 100 row_number() OVER (ORDER BY (select 1)) as id
	from sys.objects
)
select * into #rn
from rn

truncate table salary
GO
-- Populate salary data
;with cteMonths as 
(
	select id
	from #rn
	where id <= 12
),
cteYears as 
(
	select 2000 + id as ID
	from #rn
	where id <= 20
)
INSERT INTO dbo.Salary  ([EmployeeId], [Month], [Year], [GrossAmount], [NetAmount], [RegularWorkAmount], [BonusAmount], [OvertimeAmount], [VacationDays], [SickLeaveDays])
select e.id as EmployeeId, m.Id as MonthId, y.Id as YearId,
--(e.id%100) + y.ID * 15 + m.id *2 + (e.SeniorityLevelId % 10) * 2846 as GrossAmount,
30000 + ABS(CHECKSUM(NewID())) % 30001 as GrossAmount,
0,0,0,0,0,0
from cteMonths m
CROSS JOIN cteYears Y
CROSS JOIN dbo.employee e
GO

select min(grossamount),max(grossamount) from dbo.salary

-- distribute sallary to net,bonus,...
update dbo.salary set NetAmount = GrossAmount * 0.9
GO
update dbo.salary set RegularWorkAmount = NetAmount * 0.8
GO
update dbo.salary set BonusAmount = NetAmount - RegularWorkAmount
where  MONTH %2 = 1
GO
update dbo.salary set OvertimeAmount = NetAmount - RegularWorkAmount
where  MONTH %2 = 0
GO

update dbo.salary set VacationDays = 10
where  MONTH = 7
GO
update dbo.salary set VacationDays = 10
where  MONTH = 12
GO
update dbo.salary set vacationDays = vacationDays + (EmployeeId % 2)
where  (employeeId + MONTH+ year)%5 = 1
GO
update dbo.salary set SickLeaveDays = EmployeeId%8, vacationDays = vacationDays + (EmployeeId % 3)
where  (employeeId + MONTH+ year)%5 = 2
GO

-- 
-- check
select * from dbo.salary 
where NetAmount <> (regularWorkAmount + BonusAmount + OverTimeAmount)

select employeeid, year,sum(vacationdays)
from dbo.salary
group by employeeid,year
order by 3 desc
