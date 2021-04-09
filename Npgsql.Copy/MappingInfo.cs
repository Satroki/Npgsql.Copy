using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
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
        public bool IsEnumProperty { get; set; }
        public EnumMappingType EnumMappingType { get; set; }

        public MappingInfo()
        {

        }

        public MappingInfo(IProperty prop, in StoreObjectIdentifier? objectIdentifier)
        {
            ColumnName = $"\"{prop.GetColumnName(objectIdentifier.Value)}\"";
            ColumnType = GetColumnType(prop);
            IsDbGenerated = prop.ValueGenerated != ValueGenerated.Never;
            IsPrimaryKey = prop.IsPrimaryKey();
            PropertyInfo = prop.PropertyInfo;
            PropertyName = prop.PropertyInfo.Name;
            IsEnumProperty = prop.ClrType.IsEnum;
            if (IsEnumProperty)
            {
                EnumMappingType = ColumnType.Contains("int")
                    ? EnumMappingType.Int
                    : EnumMappingType.String;
            }
        }

        private string GetColumnType(IProperty property)
        {
            var type = property.GetColumnType();
            //if (type.StartsWith("timestamp", StringComparison.OrdinalIgnoreCase))
            //    return "timestamp";
            //if (type.StartsWith("time", StringComparison.OrdinalIgnoreCase))
            //    return "time";
            //if (type.StartsWith("character", StringComparison.OrdinalIgnoreCase))
            //    return "character";
            return type;
        }
    }

    public enum EnumMappingType
    {
        Int,
        String
    }
}
