// See https://aka.ms/new-console-template for more information
// Console.WriteLine("Hello, World!");

// dotnet add package ICSharpCode.SharpZipLib
// dotnet add package SharpCompress;
// dotnet add package System.Data.SQLite

using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Globalization; //for CultureInfo
using SharpCompress.Archives;
using SharpCompress.Common;
using System.Data.SQLite;
using System.Data.SqlClient;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.VisualBasic.FileIO;
using System.Collections.Generic;

namespace DataImportApp
{
    class Program //TODO: make this a static class
    {
        /* Made downloadDataFromUrlAndCreateOutputFile a static method because 
        it does not need to access or modify any instance-specific state, 
        you can call the method without creating an instance of the enclosing class. */
        static async Task downloadCompressedDataFromUrlAndCreateOutputFile(string url, string outputFileName){
            // ==========================================================================
            // Create a HTTP request to the github zip file online
            using (var client = new HttpClient()){
                // Send the GET request and retrieve the file content as a stream
                var response = await client.GetAsync(url);
                response.EnsureSuccessStatusCode();
                var fileStream = await response.Content.ReadAsStreamAsync();
            // ==========================================================================
                // Save the file to the specified path of outputFileName
                using (var fileStreamOutput = File.Create(outputFileName))
                {
                    await fileStream.CopyToAsync(fileStreamOutput);
                }
            }
        }
        static void uncompressOutputFileContentsToExtractedFolderPath(string outputFileName, string extractedFolderPath)
        {
            try{
                using (var archive = ArchiveFactory.Open(outputFileName))
                            {
                                foreach (var entry in archive.Entries)
                                {
                                    if (!entry.IsDirectory)
                                    {
                                        entry.WriteToDirectory(extractedFolderPath, new ExtractionOptions { ExtractFullPath = true, Overwrite = true });
                                    }
                                }
                            }
            }
            catch (Exception ex){
                Console.WriteLine($"Error occurred while extracting the archive: {ex.Message}");
            }
            
        }
       static string customizeDateFormat(string input, string inputFormat, string outputFormat){
            try{
                string output;
                DateTime date = DateTime.ParseExact(input, inputFormat, CultureInfo.InvariantCulture);
                output = date.ToString(outputFormat);
                return output;
            }
            catch (Exception ex){
                Console.WriteLine($"Error occurred while customizing date format: {ex.Message}");
                return "";
            }

            // Code below will not throw exception if it can't parse the date, but rather return an empty string. We want exception thrown for debugging
            // if (DateTime.TryParseExact(input, inputFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
            // {
            //     return date.ToString(outputFormat);
            // }
            // return string.Empty; // default value
       }
       static void createSQLTable(SQLiteConnection connection, string create_table_sql_command){
           try{
                // Create SQL Table
                using (var command = new SQLiteCommand(create_table_sql_command, connection))
                {
                    Console.WriteLine("SQL Table Created");
                    command.ExecuteNonQuery();
                }    
           }
           catch (Exception ex){
               Console.WriteLine($"Error occurred while creating SQL Table: {ex.Message}");
           }
       }
        // TODO: In the future, we can make this a insertDataIntoSqlTable and add different file formats other than csv
       static void insertCSVIntoSQLTable(string csvFilePath, SQLiteConnection connection, string SQL_TABLE_NAME, List<KeyValuePair<string, string>> sqlTableColumnNameAndType, bool hasHeaders){
            try{
                
                List<string[]> rows = new List<string[]>();
                
                // TextFieldParser deals with double quote cases where there are commas in the double quotes
                using (TextFieldParser parser = new TextFieldParser(csvFilePath))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");
    
                    // Check if the CSV file has headers
                    if (hasHeaders)
                    {
                        // Skip the first line (header line)
                        parser.ReadLine();
                    }
                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();
                        
                        // Process the fields
                        if (fields != null) rows.Add(fields);

                        /* TODO: ADD CODE FOR DETECTING NULL VALUES IN THE FUTURE
                        // string[] fields = parser.ReadFields();
                        // if (fields != null)
                        // {
                        //     // Process the fields
                        //     rows.Add(fields);
                        // }
                        // else{
                        //     Console.WriteLine("ENCOUNTERED NULL IN TABLE");
                        //     continue;
                        // }
                        */
                    }
                }

                // Set up Insert Query Statement
                List<string> tableColumns = new List<string>();
                foreach (var columnNameAndType in sqlTableColumnNameAndType)
                {
                    string columnName = columnNameAndType.Key;
                    tableColumns.Add(columnName);
                }
                string columnNames = string.Join(", ", tableColumns);
                string parameterNames = string.Join(", ", tableColumns.Select(c => $"@{c}"));
                string query = $"INSERT INTO {SQL_TABLE_NAME} ({columnNames}) VALUES ({parameterNames})";
                int pointerToTableColumn = 0;

                // Access the parsed data fields of each row
                foreach (string[] row in rows)
                {
                    using (var command = new SQLiteCommand(query, connection))
                    {
                        foreach (string field in row)
                        {
                            if (sqlTableColumnNameAndType[pointerToTableColumn].Value == "DATE"){
                                // In this case, we know the input date format in csv is "yyyy-MM-dd" and we want it to appear as "yyyy-MM-dd" in our SQL Table
                                command.Parameters.AddWithValue($"@{tableColumns[pointerToTableColumn]}", customizeDateFormat(field, "yyyy-MM-dd", "yyyy-MM-dd"));
                            }
                            else{
                                command.Parameters.AddWithValue($"@{tableColumns[pointerToTableColumn]}", field);
                            }
                            pointerToTableColumn++;
                        }
                        pointerToTableColumn = 0;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex){
                Console.WriteLine($"Error occurred while inserting CSV into SQL Table: {ex.Message}");
            }
            /*
                Previous Read CSV code: I thought the instructions wanted double quotes in text field,
                but realized later on that certain fields have a comma in double quotes and would get cut off
                 "Designer, ceramics/pottery" and therefore must be properly parsed

                // // Read the CSV file and insert the data into the table
                // using (var reader = new StreamReader(csvFilePath))
                // {   
                //     // Read and throw away the first line to skip the column names
                //     Console.WriteLine($"Headers of CSV: {reader.ReadLine()}");

                //     string? line; // ? allows for Nullable<string>
                //     while ((line = reader.ReadLine()) != null)
                //     {
                //         string[] columns = line.Split(',');

                //         // Could use indexOf and pass an array of arguments for column names using (params string[] args) or (List<KeyValuePair<string, string>)
                //         // I feel like having a query string is more readable, however, it prevents extensibility.
                //         // string query = $"INSERT INTO Users (UserIndex, UserId, FirstName, LastName, Sex, Email, Phone, DateOfBirth, JobTitle) VALUES (@UserIndex, @UserId, @FirstName, @LastName, @Sex, @Email, @Phone, @DateOfBirth, @JobTitle)";
                        
                //         // I think having the code below is less readable than the code above, but it allows for better extensibility of code
                //         List<string> tableColumns = new List<string>();
                //         foreach (var columnNameAndType in sqlTableColumnNameAndType)
                //         {
                //             string columnName = columnNameAndType.Key;
                //             tableColumns.Add(columnName);
                //         }
                //         string columnNames = string.Join(", ", tableColumns);
                //         string parameterNames = string.Join(", ", tableColumns.Select(c => $"@{c}"));
                //         string query = $"INSERT INTO {SQL_TABLE_NAME} ({columnNames}) VALUES ({parameterNames})";

                //         using (var command = new SQLiteCommand(query, connection))
                //         {

                //             // BEFORE, adding each column's value was explicitly typed out, even though this is more readable, it prevents future extensibility
                //             // Parameterized Queries prevent SQL Injection
                //             // command.Parameters.AddWithValue("@UserIndex", columns[0]);
                //             // command.Parameters.AddWithValue("@UserId", $"\"{columns[1]}\"");
                //             // command.Parameters.AddWithValue("@FirstName", $"\"{columns[2]}\"");
                //             // command.Parameters.AddWithValue("@LastName", $"\"{columns[3]}\"");
                //             // command.Parameters.AddWithValue("@Sex", $"\"{columns[4]}\"");
                //             // command.Parameters.AddWithValue("@Email", $"\"{columns[5]}\"");
                //             // command.Parameters.AddWithValue("@Phone", $"\"{columns[6]}\"");
                //             // command.Parameters.AddWithValue("@DateOfBirth", columns[7]);
                //             // command.Parameters.AddWithValue("@JobTitle", $"\"{columns[8]}\"");

                //             for (int i = 0; i < tableColumns.Count; i++)
                //             {
                //                 if (sqlTableColumnNameAndType[i].Value == "VARCHAR(100)"){ // TODO: might want to add STRING type along with VARCHAR as well in the future
                //                     // If type is a string, then add double quotes around it
                //                     command.Parameters.AddWithValue($"@{tableColumns[i]}", $"\"{columns[i]}\"");
                //                 }
                //                 else if (sqlTableColumnNameAndType[i].Value == "DATE"){
                //                     // In this case, we know the input date format in csv is "yyyy-MM-dd" and we want it to appear as "yyyy-MM-dd" in our SQL Table
                //                     command.Parameters.AddWithValue($"@{tableColumns[i]}", customizeDateFormat(columns[i], "yyyy-MM-dd", "yyyy-MM-dd"));
                //                 }
                //                 else {
                //                     // If type is Integer or anything else, just add the value to the table
                //                     command.Parameters.AddWithValue($"@{tableColumns[i]}", columns[i]);
                //                 }
                //             }
                            
                //             command.ExecuteNonQuery();
                //         }
                //     }
                // }
            */
       }
        static void Main(string[] args)
        {
           
            // Declare the url link to get compressed data and the outputFilePath for the compressed data
            string url = "https://github.com/datablist/sample-csv-files/raw/main/files/people/people-1000.zip";
            string outputFileName = Path.Combine(Directory.GetCurrentDirectory(),"people-1000.zip"); // @"/Users/laetitiahuang/Desktop/app/people-1000.zip";
            // Call the downloadCompressedDataFromUrlAndCreateOutputFile() method and await its completion
            downloadCompressedDataFromUrlAndCreateOutputFile(url, outputFileName).GetAwaiter().GetResult();

            // ==========================================================================
            // Create Extracted Folder in app folder to store contents of unzipped file
            string extractedFolderPath = Path.Combine(Directory.GetCurrentDirectory(),"people-1000"); // @"/Users/laetitiahuang/Desktop/app/people-1000/"; // Specify the folder path where you want to extract the files
            Directory.CreateDirectory(extractedFolderPath);
            Console.WriteLine($"Folder Created at Path: {extractedFolderPath}");

            // ==========================================================================
            // Extract the contents of the ZIP file
            uncompressOutputFileContentsToExtractedFolderPath(outputFileName, extractedFolderPath);

            // ==========================================================================
            // Step 2: Read and insert data into the table

            // Create databasePath
            string databasePath = Path.Combine(Directory.GetCurrentDirectory(),"people_database.db"); 
            Console.WriteLine($"Database Path: {databasePath}");

            bool isCreateSQLTable = false; // Assume that SQL Table is already created in databasePath file

            // Create a database file if it does not exist
            if (!File.Exists(databasePath))
            {
                SQLiteConnection.CreateFile(databasePath);
                isCreateSQLTable = true;
            }
            
            // Table Names should be constants
            const string SQL_TABLE_NAME = "Users";

            // Since the Table Columns are known beforehand and is usually fixed size, we used array of key value pairs instead of dictionary
            // Simplicity is Better
            // The Key Value Pair will be {columnName, columnType}
            List<KeyValuePair<string, string>> usersTableColumnNameAndType = new List<KeyValuePair<string, string>>();
            // Adding key-value pairs to the list
            // Column Names: UserIndex, UserId, FirstName, LastName, Sex, Email, Phone, DateOfBirth, JobTitle
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("UserIndex", "INT"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("UserId", "VARCHAR(100)"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("FirstName", "VARCHAR(100)"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("LastName", "VARCHAR(100)"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("Sex", "VARCHAR(100)"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("Email", "VARCHAR(100)"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("Phone", "VARCHAR(100)"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("DateOfBirth", "DATE"));
            usersTableColumnNameAndType.Add(new KeyValuePair<string, string>("JobTitle", "VARCHAR(100)"));
        
            /* TODO: In the future, we can add multiple groups of csvFilePath, SQL_TABLE_NAME, the tables column name 
            and type as a list of key value pairs, and the create table statement together in a list of lists and pass it 
            to this framework to automate inserting mulitiple data formats, not just csv, into the database */

            // Otherwise, connect to the databasePath
            // If you want to connect to MSSQL change SQLiteConnection to SqlConnection and SQLiteCommand to SqlCommand, and connection string to
            //  string connectionString = "Data Source=<ea9b1092f709>;Initial Catalog=<master>;User ID=<sa>;Password=<reallyStrongPwd123>;";
            string connectionString = $"Data Source={databasePath};Version=3;";
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();
                
                if (isCreateSQLTable){
                    // Create SQL Table
                    string create_table_sql_command = @"CREATE TABLE IF NOT EXISTS Users(
                                                        UserIndex INT NOT NULL,
                                                        UserId VARCHAR(100) PRIMARY KEY NOT NULL,
                                                        FirstName VARCHAR(100) NOT NULL,
                                                        LastName VARCHAR(100) NOT NULL,
                                                        Sex VARCHAR(100) NOT NULL,
                                                        Email VARCHAR(100) NOT NULL,
                                                        Phone VARCHAR(100) NOT NULL,
                                                        DateOfBirth DATE NOT NULL,
                                                        JobTitle VARCHAR(100) NOT NULL
                                                        );";
                    createSQLTable(connection, create_table_sql_command);
                }

                // Get CSV file path from extracted folder
                string csvFilePath = Path.Combine(extractedFolderPath, "people-1000.csv"); 
                Console.WriteLine($"CSV file Path: {csvFilePath}");
                 // Read the CSV file and insert the data into the table
                insertCSVIntoSQLTable(csvFilePath, connection, SQL_TABLE_NAME, usersTableColumnNameAndType, true);
            
                connection.Close(); 
            }
        }
    }
}
// ==========================================================================
// Testing Customize Date Format Function
// string input = "07-28-2023";
// string inputFormat = "MM-dd-yyyy";
// string outputFormat = "yyyy-MM-dd";
// string output;
// output = customizeDateFormat(input, inputFormat, outputFormat);
// Console.WriteLine(output);
// var dateFormatProvider = new CultureInfo("en-US");
// string d1 = "12-02-2020";
// string dateOfBirth = DateTime.ParseExact(d1.Trim('"'), "yyyy-MM-dd", dateFormatProvider).ToString("yyyy-MM-dd");
// Console.WriteLine($"Date of Birth: {dateOfBirth}");

// ==========================================================================

/*
Step 3:

// Step 3 Part 1 MS SQL: USING PIVOT KEYWORD [dbo].[people-1000] is Users Table in the sqlite expression
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

Result:
Category        Male        Female
Age By Gender	56.250988	60.427125

// Step 3 Part 2 MS SQL: [dbo].[people-1000] is Users Table in the sqlite expression
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
LEFT JOIN SexToGenderAvg ON Subquery.Sex = SexToGenderAvg.Sex

// Look at csv file for results

// Now we move on to sqlite

(base) Laetitias-MacBook-Air:app laetitiahuang$ sqlite3 /Users/laetitiahuang/Desktop/app/people_database.db
SQLite version 3.36.0 2021-06-18 18:58:49
Enter ".help" for usage hints.
sqlite> .tables
Users
sqlite> .mode columns
sqlite> .headers yes

// Step 3 Part 1 in SQLite:

SELECT
    AVG(CASE WHEN Sex = 'Male' THEN Age END) AS MaleAverageAge,
    AVG(CASE WHEN Sex = 'Female' THEN Age END) AS FemaleAverageAge
FROM (
    SELECT
        Sex,
        (julianday('2023-01-01') - julianday(DateOfBirth))/365 AS Age
    FROM Users
) AS AgeData;

Results:
MaleAverageAge    FemaleAverageAge
----------------  ----------------
55.7766744274189  59.9848150407632
*/

/*
// Step 3 Part 2 in SQLite:

// USING CROSS JOIN SOLUTION

WITH CTE AS(
SELECT
AVG(CASE WHEN Sex = 'Male' THEN Age END) AS MaleAverageAge,
AVG(CASE WHEN Sex = 'Female' THEN Age END) AS FemaleAverageAge
FROM (
   SELECT
   Sex,
   (julianday('2023-01-01') - julianday(DateOfBirth))/365 AS Age
   FROM Users
) AS AgeData)
SELECT UserId, Age, CASE WHEN Sex = 'Male' THEN Age - MaleAverageAge
WHEN Sex = 'Female' THEN Age - FemaleAverageAge END AS AgeMinusGenderAverage
FROM (SELECT UserId, Sex, (julianday('2023-01-01') - julianday(DateOfBirth))/365 AS Age 
FROM Users) as Subquery 
CROSS JOIN 
CTE 
LIMIT 5;

Results:
UserId             Age               AgeMinusGenderAverage
-----------------  ----------------  ---------------------
"8717bbf45cCDbEe"  8.93424657534247  -46.8424278520764    
"3d5AD30A4cD38ed"  91.4986301369863  31.5138150962231     
"810Ce0F276Badec"  9.10684931506849  -50.8779657256947    
"BF2a889C00f0cE1"  10.1287671232877  -45.6479073041312    
"9afFEafAe1CBBB9"  99.7835616438356  39.7987466030724

// SEE THE REST IN people.txt

// USING LEFT JOIN SOLUTION

WITH AverageAgeByGender AS (
    SELECT
        AVG(CASE WHEN Sex = 'Male' THEN Age END) AS MaleAverageAge,
        AVG(CASE WHEN Sex = 'Female' THEN Age END) AS FemaleAverageAge,
        Age,
        Sex
    FROM (
       SELECT
           (julianday('2023-01-01') - julianday(DateOfBirth))/365 AS Age,
           Sex
       FROM Users
    ) AS AgeData
),
SexToGenderAvg AS (
    SELECT 'Male' AS Sex, MaleAverageAge AS AverageAge
    FROM AverageAgeByGender
    UNION ALL
    SELECT 'Female' AS Sex, FemaleAverageAge AS AverageAge
    FROM AverageAgeByGender
)
SELECT UserId, Age, CASE WHEN Subquery.Sex = 'Male' THEN  Age - AverageAge
                       WHEN Subquery.Sex = 'Female' THEN Age - AverageAge
                  END AS AgeMinusGenderAverage
FROM (SELECT UserId, (julianday('2023-01-01') - julianday(DateOfBirth))/365 AS Age, Sex
FROM Users ) as Subquery
LEFT JOIN SexToGenderAvg ON Subquery.Sex = SexToGenderAvg.Sex
LIMIT 5;
*/