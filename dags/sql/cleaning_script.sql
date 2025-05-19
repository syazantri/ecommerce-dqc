CREATE TABLE IF NOT EXISTS cleaned_products AS
SELECT
    ItemId,
    Channel,
    ScrapDate,
    CAST(NULLIF(SalesCount, '') AS INTEGER) AS SalesCount,
    CAST(FLOOR(CAST(NULLIF(SalePrice, '') AS FLOAT)) AS INTEGER) AS SalePrice,
    CAST(FLOOR(CAST(NULLIF(OriginalPrice, '') AS FLOAT)) AS INTEGER) AS OriginalPrice,
    CAST(FLOOR(CAST(NULLIF(Discount, '') AS FLOAT)) AS INTEGER) AS Discount
FROM raw_products
WHERE
    SalePrice IS NOT NULL
    AND SalePrice != '0'
    AND OriginalPrice IS NOT NULL
    AND OriginalPrice != '0';
