using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Adf_Upsert_Sql.Helpers
{
    public static class SqlDataTypeHelper
    {
        public static object ConvertCsvDataToSqlType(string csvData, SqlDbType sqlDbType)
        {
            if (string.IsNullOrEmpty(csvData))
            {
                return DBNull.Value;
            }

            switch (sqlDbType)
            {
                case SqlDbType.Int:
                    if (int.TryParse(csvData, out int intResult))
                    {
                        return intResult;
                    }
                    break;

                case SqlDbType.BigInt:
                    if (long.TryParse(csvData, out long bigIntResult))
                    {
                        return bigIntResult;
                    }
                    break;

                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    if (decimal.TryParse(csvData, out decimal decimalResult))
                    {
                        return decimalResult;
                    }
                    break;

                case SqlDbType.Float:
                    if (double.TryParse(csvData, out double floatResult))
                    {
                        return floatResult;
                    }
                    break;

                case SqlDbType.Real:
                    if (float.TryParse(csvData, out float realResult))
                    {
                        return realResult;
                    }
                    break;

                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.Date:
                    if (DateTime.TryParse(csvData, out DateTime dateTimeResult))
                    {
                        return dateTimeResult;
                    }
                    break;

                case SqlDbType.Bit:
                    if (bool.TryParse(csvData, out bool boolResult))
                    {
                        return boolResult;
                    }
                    else if (csvData == "1" || csvData.Equals("true", StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                    else if (csvData == "0" || csvData.Equals("false", StringComparison.OrdinalIgnoreCase))
                    {
                        return false;
                    }
                    break;

                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.Text:
                case SqlDbType.NText:
                    return csvData; // Directly assign string

                case SqlDbType.UniqueIdentifier:
                    if (Guid.TryParse(csvData, out Guid guidResult))
                    {
                        return guidResult;
                    }
                    break;

                case SqlDbType.Binary:
                case SqlDbType.VarBinary:
                case SqlDbType.Image:
                    try
                    {
                        return Convert.FromBase64String(csvData); // Assuming Base64 encoding for binary data
                    }
                    catch
                    {
                        throw new FormatException($"Cannot convert '{csvData}' to {sqlDbType}. Expected Base64 encoded string.");
                    }

                default:
                    throw new InvalidOperationException($"Unhandled SqlDbType: {sqlDbType}");
            }

            // If parsing fails for the provided type
            throw new FormatException($"Cannot convert '{csvData}' to {sqlDbType}");
        }

        public static SqlDbType GetTypeCodes(string sqlType)
        {
            switch (sqlType.ToLower()) // Normalize to lowercase for consistency
            {
                case "bigint":
                    return SqlDbType.BigInt;
                case "binary":
                    return SqlDbType.Binary;
                case "bit":
                    return SqlDbType.Bit;
                case "char":
                    return SqlDbType.Char;
                case "date":
                    return SqlDbType.Date;
                case "datetime":
                    return SqlDbType.DateTime;
                case "datetime2":
                    return SqlDbType.DateTime2;
                case "datetimeoffset":
                    return SqlDbType.DateTimeOffset;
                case "decimal":
                    return SqlDbType.Decimal;
                case "float":
                    return SqlDbType.Float;
                case "image":
                    return SqlDbType.Image;
                case "int":
                    return SqlDbType.Int;
                case "money":
                    return SqlDbType.Money;
                case "nchar":
                    return SqlDbType.NChar;
                case "ntext":
                    return SqlDbType.NText;
                case "nvarchar":
                    return SqlDbType.NVarChar;
                case "real":
                    return SqlDbType.Real;
                case "smalldatetime":
                    return SqlDbType.SmallDateTime;
                case "smallint":
                    return SqlDbType.SmallInt;
                case "smallmoney":
                    return SqlDbType.SmallMoney;
                case "text":
                    return SqlDbType.Text;
                case "time":
                    return SqlDbType.Time;
                case "timestamp":
                    return SqlDbType.Timestamp;
                case "tinyint":
                    return SqlDbType.TinyInt;
                case "uniqueidentifier":
                    return SqlDbType.UniqueIdentifier;
                case "varbinary":
                    return SqlDbType.VarBinary;
                case "varchar":
                    return SqlDbType.VarChar;
                case "xml":
                    return SqlDbType.Xml;
                default:
                    throw new ArgumentException("Unhandled SQL type: " + sqlType);
            }
        }
    }

}
