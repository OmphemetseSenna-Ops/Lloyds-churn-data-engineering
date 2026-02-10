CREATE DATABASE CustomerChurnDB;
GO
USE CustomerChurnDB;
GO

CREATE TABLE [ETL Process Log] (
    [Log ID] INT IDENTITY(1,1) PRIMARY KEY,
    [Process Name] VARCHAR(100),
    [Start Time] DATETIME,
    [End Time] DATETIME,
    [Status] VARCHAR(20),
    [Records Inserted] INT,
    Message VARCHAR(500)
);

CREATE TABLE [ETL Error Log] (
    [Error ID] INT IDENTITY(1,1) PRIMARY KEY,
    [Process Name] VARCHAR(100),
    [Error Time] DATETIME,
    [Error Step] VARCHAR(100),
    [Error Message] VARCHAR(1000)
);


