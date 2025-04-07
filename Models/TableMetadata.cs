using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Adf_Upsert_Sql.Models
{
    public class TableMetadata
    {
        public string tableName { get; set; }
        public string schemaName { get; set; }

        public string primaryColumn { get; set; }

        public List<Row> rows { get; set; }
    }


    public class Row
    {
        public List<SqlColumn> columns { get; set; }


        public List<SqlColumn> GetRecordsExcludingPrimaryKeys()
        {
            var nonPrimaryKeys = new List<SqlColumn>();

            foreach (var record in columns)
            {
                if (!record.IsPrimaryKey)
                {
                    nonPrimaryKeys.Add(record);
                }
            }
            return nonPrimaryKeys;
        }
        public List<SqlColumn> GetPrimaryKeys()
        {
            var primaryKeys = new List<SqlColumn>();
            foreach (var record in columns)
            {
                if (record.IsPrimaryKey)
                {
                    primaryKeys.Add(record);
                }
            }
            return primaryKeys;
        }
    }

    
}
