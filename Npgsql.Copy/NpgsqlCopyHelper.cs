using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Npgsql.Copy
{
    public class NpgsqlCopyHelper<T> where T : class
    {
        private readonly string tableName;
        private readonly string schema;
        private readonly List<MappingInfo> mappingList = new List<MappingInfo>();
        private readonly bool autoTransactions;

        public NpgsqlCopyHelper(DbContext context, bool autoTransactions = true)
        {
            var type = context.Model.FindEntityType(typeof(T));
            var r = type.Relational();
            tableName = $"\"{r.TableName}\"";
            schema = r.Schema;
            var props = type.GetProperties();
            foreach (var prop in props)
            {
                var mi = new MappingInfo(prop);
                mappingList.Add(mi);
            }

            this.autoTransactions = autoTransactions;
        }

        public int Insert(NpgsqlConnection conn, IEnumerable<T> list, NpgsqlTransaction tran = null)
        {
            if (list == null || !list.Any())
                return 0;
            var mis = mappingList.Where(mi => !mi.IsDbGenerated).ToList();
            using (tran = GetTransaction(conn, tran))
            {
                try
                {
                    var count = CopyData(conn, list, mis, tableName);
                    tran?.Commit();
                    return count;
                }
                catch (Exception e)
                {
                    tran?.Rollback();
                    throw e;
                }
            }
        }

        public Task<int> InsertAsync(NpgsqlConnection conn, IEnumerable<T> list, NpgsqlTransaction tran = null)
        {
            if (list == null || !list.Any())
                return Task.FromResult(0);
            return Task.Run(() => Insert(conn, list, tran));
        }

        public int Update(NpgsqlConnection conn, IEnumerable<T> list, NpgsqlTransaction tran = null, IEnumerable<string> updateFields = null)
        {
            if (list == null || !list.Any())
                return 0;
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
            using (tran = GetTransaction(conn, tran))
            {
                try
                {
                    var temp = $"temp_{tableName.Trim('"').ToLower()}_{DateTime.Now.Ticks}";
                    var sql = $"CREATE TEMP TABLE {temp} (LIKE {tableName} INCLUDING DEFAULTS) ON COMMIT DROP";
                    new NpgsqlCommand(sql, conn, tran).ExecuteNonQuery();

                    CopyData(conn, list, mappings, temp);

                    var mis = mappings.Where(m => !m.IsPrimaryKey).ToList();
                    var updateCols = string.Join(",", mis.Select(m => $"{m.ColumnName} = source.{m.ColumnName}"));
                    var pk = mappings.First(m => m.IsPrimaryKey).ColumnName;
                    var updateSql = $"UPDATE {tableName} SET {updateCols} FROM {temp} as source WHERE {tableName}.{pk}=source.{pk}";
                    var count = new NpgsqlCommand(updateSql, conn, tran).ExecuteNonQuery();
                    tran?.Commit();
                    return count;
                }
                catch (Exception e)
                {
                    tran?.Rollback();
                    throw e;
                }
            }
        }

        public Task<int> UpdateAsync(NpgsqlConnection conn, IEnumerable<T> list, NpgsqlTransaction tran = null, IEnumerable<string> updateFields = null)
        {
            if (list == null || !list.Any())
                return Task.FromResult(0);
            return Task.Run(() => Update(conn, list, tran, updateFields));
        }

        private int CopyData(NpgsqlConnection conn, IEnumerable<T> list, List<MappingInfo> mis, string table)
        {
            var cols = string.Join(",", mis.Select(m => m.ColumnName));
            var count = 0;
            using (var importer = conn.BeginBinaryImport($"COPY {table} ({cols}) FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var i in list)
                {
                    importer.StartRow();
                    count++;
                    foreach (var p in mis)
                    {
                        var v = p.PropertyInfo.GetValue(i);
                        if (v == null)
                            importer.WriteNull();
                        else
                        {
                            if (p.IsEnumProperty)
                            {
                                switch (p.EnumMappingType)
                                {
                                    case EnumMappingType.Int:
                                        importer.Write((int)v, p.ColumnType);
                                        break;
                                    case EnumMappingType.String:
                                        importer.Write(v.ToString(), p.ColumnType);
                                        break;
                                }
                            }
                            else
                            {
                                switch (v)
                                {
                                    case DateTimeOffset dt:
                                        importer.Write(dt.DateTime, p.ColumnType);
                                        break;
                                    default:
                                        importer.Write(v, p.ColumnType);
                                        break;
                                }
                            }
                        }
                    }
                }
                importer.Complete();
            }
            return count;
        }

        private void EnsureConnectionOpened(NpgsqlConnection conn)
        {
            if (conn.State != System.Data.ConnectionState.Open)
            {
                conn.Open();
            }
        }

        private NpgsqlTransaction GetTransaction(NpgsqlConnection conn, NpgsqlTransaction tran)
        {
            EnsureConnectionOpened(conn);
            if (tran == null && autoTransactions)
            {
                return conn.BeginTransaction();
            }
            return tran;
        }
    }
}
