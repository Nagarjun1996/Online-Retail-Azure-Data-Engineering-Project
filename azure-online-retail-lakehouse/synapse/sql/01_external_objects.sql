CREATE EXTERNAL DATA SOURCE OnlineRetail
WITH (
    LOCATION = 'abfss://<container>@<account>.dfs.core.windows.net',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
CREATE EXTERNAL FILE FORMAT delta_format WITH (FORMAT_TYPE = DELTA);
