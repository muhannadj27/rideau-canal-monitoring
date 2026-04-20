-- Rideau Canal Skateway - Stream Analytics Query
-- Aggregates sensor data in 5-minute tumbling windows
-- Outputs to both Cosmos DB and Blob Storage

-- Output to Cosmos DB (live dashboard)
SELECT
    location,
    System.Timestamp() AS windowend,
    CONCAT(location, '-', CAST(System.Timestamp() AS nvarchar(max))) AS id,
    AVG(iceThickness) AS avgicethickness,
    MIN(iceThickness) AS minicethickness,
    MAX(iceThickness) AS maxicethickness,
    AVG(surfaceTemperature) AS avgsurfacetemperature,
    MIN(surfaceTemperature) AS minsurfacetemperature,
    MAX(surfaceTemperature) AS maxsurfacetemperature,
    MAX(snowAccumulation) AS maxsnowaccumulation,
    AVG(externalTemperature) AS avgexternaltemperature,
    COUNT(*) AS readingcount,
    CASE
        WHEN AVG(iceThickness) >= 30 AND AVG(surfaceTemperature) <= -2 THEN 'Safe'
        WHEN AVG(iceThickness) >= 25 AND AVG(surfaceTemperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetystatus
INTO [CosmosDBOutput]
FROM [IoTHubInput]
TIMESTAMP BY CAST(timestamp AS datetime)
GROUP BY location, TumblingWindow(minute, 5)

-- Output to Blob Storage (historical archive)
SELECT
    location,
    System.Timestamp() AS windowend,
    CONCAT(location, '-', CAST(System.Timestamp() AS nvarchar(max))) AS id,
    AVG(iceThickness) AS avgicethickness,
    MIN(iceThickness) AS minicethickness,
    MAX(iceThickness) AS maxicethickness,
    AVG(surfaceTemperature) AS avgsurfacetemperature,
    MIN(surfaceTemperature) AS minsurfacetemperature,
    MAX(surfaceTemperature) AS maxsurfacetemperature,
    MAX(snowAccumulation) AS maxsnowaccumulation,
    AVG(externalTemperature) AS avgexternaltemperature,
    COUNT(*) AS readingcount,
    CASE
        WHEN AVG(iceThickness) >= 30 AND AVG(surfaceTemperature) <= -2 THEN 'Safe'
        WHEN AVG(iceThickness) >= 25 AND AVG(surfaceTemperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS safetystatus
INTO [BlobOutput]
FROM [IoTHubInput]
TIMESTAMP BY CAST(timestamp AS datetime)
GROUP BY location, TumblingWindow(minute, 5)
