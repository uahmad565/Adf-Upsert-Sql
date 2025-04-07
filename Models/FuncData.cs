using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Adf_Upsert_Sql.Models
{
    public record FuncData(string schema, string tableName, string primaryColumn,string triggerStart);
}
