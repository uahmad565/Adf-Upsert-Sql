using Adf_Upsert_Sql.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Adf_Upsert_Sql.Helpers
{
    internal class SqlSchemaHelper
    {



        public static Dictionary<string, SqlColumn> GetColumnMetaDeta(string connectionString, string tableName, string schemaName)
        {

            string query = @$"
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH
        FROM 
            INFORMATION_SCHEMA.COLUMNS
        WHERE 
            TABLE_NAME = '{tableName}' AND
            TABLE_SCHEMA = '{schemaName}'";

            var columnsMetadata = new Dictionary<string, SqlColumn>();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var columnMetadata = new SqlColumn
                        {
                            ColumnName = reader["COLUMN_NAME"].ToString(),
                            DataType = SqlDataTypeHelper.GetTypeCodes(reader["DATA_TYPE"].ToString()),
                            CharacterMaximumLength = reader["CHARACTER_MAXIMUM_LENGTH"] as int? // Handle possible null values as nullable int
                        };

                        columnsMetadata[columnMetadata.ColumnName] = columnMetadata;
                    }
                }
            }
            return columnsMetadata;
        }
    }
}
