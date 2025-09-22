IF OBJECT_ID('dbo.OnlineRetail_dw', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.OnlineRetail_dw;
CREATE EXTERNAL TABLE dbo.OnlineRetail_dw
(
    Invoice       NVARCHAR(50),
    StockCode     NVARCHAR(50),
    Description   NVARCHAR(2000),
    Quantity      INT,
    InvoiceDate   DATETIME2,
    Price         FLOAT,
    CustomerID    NVARCHAR(400),
    Country       NVARCHAR(100),
    Year          INT,
    Month         INT,
    Revenue       FLOAT
)
WITH
(
    LOCATION = 'gold/online_retail/orders_silver',
    DATA_SOURCE = OnlineRetail,
    FILE_FORMAT = delta_format
);

IF OBJECT_ID('dbo.OnlineRetail_RevenueByCountry', 'U') IS NOT NULL
    DROP EXTERNAL TABLE dbo.OnlineRetail_RevenueByCountry;
CREATE EXTERNAL TABLE dbo.OnlineRetail_RevenueByCountry
(
    Country NVARCHAR(100),
    TotalRevenue FLOAT,
    NumRows BIGINT
)
WITH
(
    LOCATION = 'gold/online_retail/mart_revenue_by_country',
    DATA_SOURCE = OnlineRetail,
    FILE_FORMAT = delta_format
);
