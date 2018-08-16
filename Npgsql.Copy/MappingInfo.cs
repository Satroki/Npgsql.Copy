using System;
using System.Collections.Generic;
using System.Text;
using NpgsqlTypes;
using Npgsql;
using System.Reflection;

namespace Npgsql.Copy
{
    public class MappingInfo
    {
        public string ColumnName { get; set; }
        public string ColumnType { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsDbGenerated { get; set; }
        public PropertyInfo PropertyInfo { get; set; }
        public string PropertyName { get; set; }
    }
}
