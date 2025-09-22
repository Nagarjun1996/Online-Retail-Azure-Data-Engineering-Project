CREATE EXTERNAL TABLE OnlineRetail_dw
(
    Invoice NVARCHAR(50),
    StockCode NVARCHAR(50),
    Description NVARCHAR(2000),
    Quantity INT,
    InvoiceDate DATETIME2,
    Price FLOAT,
    CustomerID VARCHAR(400),
    Country NVARCHAR(100),
    Year INT,
    Month INT,
    Revenue FLOAT
)
WITH
(
    LOCATION = 'Data_warehouse',
    DATA_SOURCE = OnlineRetail,
    FILE_FORMAT = delta_format
);


SELECT * FROM OnlineRetail_dw
