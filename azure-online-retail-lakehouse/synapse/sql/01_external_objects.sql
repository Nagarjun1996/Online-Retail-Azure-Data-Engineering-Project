---------------------
-- CREATE CREDENTIAL 
---------------------

CREATE DATABASE SCOPED CREDENTIAL arjuncred
WITH
    IDENTITY = 'Managed Identity'

-------------------------------
-- CREATE EXTERNAL DATA SOURCE 
-------------------------------
CREATE EXTERNAL DATA SOURCE OnlineRetail
WITH
(
    LOCATION = 'https://onlineretail96.dfs.core.windows.net/gold' ,
    CREDENTIAL = arjuncred
)

------------------
-- OPEN ROW SET 
------------------

SELECT 
*
FROM
    OPENROWSET(
            BULK 'Data_warehouse',
            DATA_SOURCE = 'OnlineRetail',
            FORMAT = 'DELTA'
    ) AS query1

-------------------------------
-- CREATE EXTERNAL FILE FORMAT 
-------------------------------

CREATE EXTERNAL FILE FORMAT delta_format
WITH(
        FORMAT_TYPE = DELTA 
)
