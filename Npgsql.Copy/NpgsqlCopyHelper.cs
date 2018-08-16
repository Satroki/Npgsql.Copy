using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql;

namespace Npgsql.Copy
{
    public class NpgsqlCopyHelper<T> where T : class
    {
        private readonly string tableName;
        private readonly string schema;
        private readonly List<MappingInfo> mappingList = new List<MappingInfo>();

        public NpgsqlCopyHelper(DbContext context)
        {
            var type = context.Model.FindEntityType(typeof(T));
            var r = type.Relational();
            tableName = $"\"{r.TableName}\"";
            schema = r.Schema;
            var props = type.GetProperties();
            foreach (var prop in props)
            {
                var mi = new MappingInfo
                {
                    ColumnName = $"\"{prop.Relational().ColumnName}\"",
                    ColumnType = prop.Relational().ColumnType,
                    IsDbGenerated = prop.ValueGenerated != ValueGenerated.Never,
                    IsPrimaryKey = prop.IsPrimaryKey(),
                    PropertyInfo = prop.PropertyInfo,
                    PropertyName = prop.PropertyInfo.Name,
                };
                mappingList.Add(mi);
            }
        }

        public void Insert(NpgsqlConnection conn, IEnumerable<T> list)
        {
            var mis = mappingList.Where(mi => !mi.IsDbGenerated).ToList();
            CopyData(conn, list, mis, tableName);
        }

        public int Update(NpgsqlConnection conn, IEnumerable<T> list, NpgsqlTransaction tran, IEnumerable<string> updateFields = null)
        {
            var mappings = mappingList;
            if (updateFields != null && updateFields.Any())
            {
                mappings = new List<MappingInfo>();
                foreach (var mi in mappingList)
                {
                    if (mi.IsPrimaryKey || updateFields.Contains(mi.PropertyName))
                        mappings.Add(mi);
                }
            }

            var temp = $"temp_{tableName.Trim('"').ToLower()}_{DateTime.Now.Ticks}";
            var sql = $"CREATE TEMP TABLE {temp} (LIKE {tableName} INCLUDING DEFAULTS) ON COMMIT DROP";
            new NpgsqlCommand(sql, conn, tran).ExecuteNonQuery();

            CopyData(conn, list, mappings, temp);

            var mis = mappings.Where(m => !m.IsPrimaryKey).ToList();
            var updateCols = string.Join(",", mis.Select(m => $"{m.ColumnName} = source.{m.ColumnName}"));
            var pk = mappings.First(m => m.IsPrimaryKey).ColumnName;
            var updateSql = $"UPDATE {tableName} SET {updateCols} FROM {temp} as source WHERE {tableName}.{pk}=source.{pk}";
            return new NpgsqlCommand(updateSql, conn, tran).ExecuteNonQuery();
        }

        private static void CopyData(NpgsqlConnection conn, IEnumerable<T> list, List<MappingInfo> mis, string table)
        {
            var cols = string.Join(",", mis.Select(m => m.ColumnName));
            using (var importer = conn.BeginBinaryImport($"COPY {table} ({cols}) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var i in list)
                {
                    importer.StartRow();
                    foreach (var p in mis)
                    {
                        var v = p.PropertyInfo.GetValue(i);
                        if (v == null)
                            importer.WriteNull();
                        else
                            importer.Write(v, p.ColumnType);
                    }
                }
                importer.Complete();
            }
        }
    }
}
