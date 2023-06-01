-- Part 1
SELECT
    'Age By Gender' AS 'Category',
    [Male],
    [Female]
FROM
(
    SELECT
        [Male],
        [Female]
    FROM
    (
        SELECT
            CASE
                WHEN Sex = 'Male' THEN 'Male'
                WHEN Sex = 'Female' THEN 'Female'
            END AS Category,
            CAST(DATEDIFF(YEAR, Date_Of_birth, '2023-01-01') AS DECIMAL(10, 2)) AS Age
        FROM [dbo].[people-1000]
    ) AS AgeData
    PIVOT
    (
        AVG(Age)
        FOR Category IN ([Male], [Female])
    ) AS PivotTable
) AS Result;

-- Part 2
WITH AverageAgeByGender (Category, MaleAverageAge, FemaleAverageAge) AS (
    SELECT
        'Age By Gender' AS 'Category',
        [Male],
        [Female]
    FROM
    (
        SELECT
            [Male],
            [Female]
        FROM
        (
            SELECT
                CASE
                    WHEN Sex = 'Male' THEN 'Male'
                    WHEN Sex = 'Female' THEN 'Female'
                END AS Category,
                CAST(DATEDIFF(YEAR, Date_Of_birth, '2023-01-01') AS DECIMAL(10, 2)) AS Age
            FROM [dbo].[people-1000]
        ) AS AgeData
        PIVOT
        (
            AVG(Age)
            FOR Category IN ([Male], [Female])
        ) AS PivotTable
    ) AS Result
),
SexToGenderAvg AS (
    SELECT 'Male' AS Sex, MaleAverageAge AS AverageAge
    FROM AverageAgeByGender
    UNION ALL
    SELECT 'Female' AS Sex, FemaleAverageAge AS AverageAge
    FROM AverageAgeByGender
)
SELECT User_Id, Age, Age - AverageAge AS AgeMinusGenderAverage
FROM (
    SELECT User_Id, DATEDIFF(YEAR, Date_Of_birth, '2023-01-01') AS Age, Sex
    FROM [dbo].[people-1000]
) AS Subquery
LEFT JOIN SexToGenderAvg ON Subquery.Sex = SexToGenderAvg.Sex;