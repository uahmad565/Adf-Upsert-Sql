using Adf_Upsert_Sql.Helpers;
using Adf_Upsert_Sql.Models;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Adf_Upsert_Sql
{
    internal class CsvToSqlOperations
    {
        private string _connectionString;

        public CsvToSqlOperations(string connectionString)
        {
            this._connectionString = connectionString;
        }
        public void GetUpdateQuery(string tableName, List<Column> columns, List<Column> primaryColumns)
        {
            string query = $"UPDATE {tableName} SET ";
            foreach (var column in columns)
            {
                query += $"{column.ColumnName} = '{column.Value}', ";
            }
            query = query.TrimEnd(',', ' ') + " WHERE";
            foreach (var column in primaryColumns)
            {

                query += string.Join(" and ", primaryColumns.Select(c => $"{c.ColumnName}={c.Value}"));
            }

            Console.WriteLine(query);
        }

        public async Task ProcessCsvFiles(string blobConnectionString, string containerName, string filePath, TableMetadata tblMeta,ILogger logger)
        {
            BlobContainerClient containerClient = new BlobContainerClient(blobConnectionString, containerName);
            var blobClient = containerClient.GetBlobClient(filePath);
            if (await blobClient.ExistsAsync())
            {
                using (Stream blobStream = blobClient.OpenRead())
                using (StreamReader reader = new StreamReader(blobStream))
                using (var csv = new CsvHelper.CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    csv.Read();
                    csv.ReadHeader();
                    var headers = csv.HeaderRecord;

                    List<Row> rows = new List<Row>();

                    while (csv.Read())
                    {
                        var columns = new List<SqlColumn>();
                        foreach (var header in headers)
                        {
                            var metaColumnDic = SqlSchemaHelper.GetColumnMetaDeta(_connectionString, tblMeta.tableName, tblMeta.schemaName);
                            var metaColumnFromSql = metaColumnDic[header];

                            var ColValue = SqlDataTypeHelper.ConvertCsvDataToSqlType(csv.GetField(header), metaColumnFromSql.DataType);
                            SqlColumn sqlColumn = new SqlColumn
                            {
                                ColumnName = header,
                                Value = ColValue,
                                DataType = metaColumnFromSql.DataType,
                                CharacterMaximumLength = metaColumnFromSql.CharacterMaximumLength, // Assuming no length restriction for simplicity
                                IsPrimaryKey = tblMeta.primaryColumn.ToLower().Equals(header.ToLower(), StringComparison.InvariantCultureIgnoreCase),
                            };
                            columns.Add(sqlColumn);

                        }
                        rows.Add(new Row { columns = columns });
                        logger.LogInformation($"Rows found {rows.Count}");
                    }
                    tblMeta.rows = rows;
                    UpsertOperation(headers, tblMeta,logger);
                }
            }
            else
            {
                throw new Exception($"Blob file {filePath} does not exist in the container {containerName}");
            }
        }
        public void UpsertOperation(string[] headers, TableMetadata tableMeta,ILogger logger)
        {
            var tableName = tableMeta.tableName;
            using (SqlConnection conn = new SqlConnection(_connectionString))
            {
                conn.Open();

                foreach (var record in tableMeta.rows)
                {
                    var primaryColumn = record.GetPrimaryKeys().FirstOrDefault();
                    if (primaryColumn == null)
                    {
                        throw new Exception("UpsertOperation() failed. Primary key not found in the record.");
                    }
                    string checkQuery = $"SELECT COUNT(*) FROM [{tableMeta.schemaName}].[{tableMeta.tableName}] WHERE {primaryColumn.ColumnName} = @{primaryColumn.ColumnName}";
                    logger.LogInformation(checkQuery);
                    using (SqlCommand checkCmd = new SqlCommand(checkQuery, conn))
                    {
                        checkCmd.Parameters.Add(new SqlParameter($"@{primaryColumn.ColumnName}", primaryColumn.DataType) { Value = primaryColumn.Value });

                        int count = (int)checkCmd.ExecuteScalar();
                        bool isUpdate = count > 0;
                        string query = isUpdate
                            ? $"UPDATE [{tableMeta.schemaName}].[{tableMeta.tableName}] SET {string.Join(", ", record.GetRecordsExcludingPrimaryKeys().Select(h => $"{h.ColumnName} = @{h.ColumnName}"))} WHERE {primaryColumn.ColumnName} = @{primaryColumn.ColumnName}"
                        : $"INSERT INTO [{tableMeta.schemaName}].[{tableMeta.tableName}] ({string.Join(", ", record.columns.Select(x => x.ColumnName))}) VALUES ({string.Join(", ", record.columns.Select(h => $"@{h.ColumnName}"))})";

                        using (SqlCommand cmd = new SqlCommand(query, conn))
                        {
                            foreach (var column in record.columns)
                            {
                                cmd.Parameters.Add(new SqlParameter($"@{column.ColumnName}", column.DataType) { Value = column.Value });
                            }
                            logger.LogInformation(query);

                            cmd.ExecuteNonQuery();
                        }


                    }
                }
            }
        }
    }
}
