using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Adf_Upsert_Sql.Models
{
    public class Column
    {
        public string ColumnName { get; set; }
        public Object? Value { get; set; }

    }

    public class SqlColumn : Column
    {
        public SqlDbType DataType { get; set; }
        public int? CharacterMaximumLength { get; set; }
        public bool IsPrimaryKey { get; set; }

    }
}
