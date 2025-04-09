using Parquet.Schema;
using ParquetMapper.Attributes;
using ParquetMapper.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Extensions
{
    public static class ParquetSchemaExtensions
    {
        public static bool CompareSchema<TDataType>(this ParquetSchema parquetSchema) where TDataType : new()
        {
            var type = new TDataType().GetType();

            return parquetSchema.CompareSchema(type);
        }
        public static bool CompareSchema(this ParquetSchema parquetSchema, Type type)
        {
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            var nullabilityContext = new NullabilityInfoContext();

            var hasIgnoreCasing = type.GetCustomAttribute<IgnoreCasingAttribute>() is null ? true : false;

            bool isValid = properties.All(prop =>
            {
                var isNullableProp = nullabilityContext.Create(prop).WriteState == NullabilityState.Nullable ? true : false;

                var isIgnorePropAttr = prop.GetCustomAttribute<IgnorePropertyAttribute>() != null ? true : false;

                if (isNullableProp | isIgnorePropAttr)
                {
                    return true;
                }

                var field = CompareWithAttributes(parquetSchema.DataFields, prop, hasIgnoreCasing);

                if (field == null)
                {
                    return false;
                }

                return field.ClrType == prop.PropertyType;
            });

            if (!isValid)
            {
                throw new IncompatibleSchemaTypeException(parquetSchema, type);
            }

            return isValid;
        }
        private static DataField? CompareWithAttributes(DataField[] dataFields, PropertyInfo property, bool hasIgnoreCasing = false)
        {

            return dataFields.FirstOrDefault(dataField =>
            {
                var fieldName = dataField.Name;
                var propertyName = property.Name;

                if (property.GetCustomAttribute<HasParquetColNameAttribute>() is HasParquetColNameAttribute colNameAttribute)
                {
                    propertyName = colNameAttribute.ColName;
                }

                if (hasIgnoreCasing || property.GetCustomAttribute<IgnoreCasingAttribute>() is IgnoreCasingAttribute)
                {
                    fieldName = fieldName.ToLower();
                    propertyName = propertyName.ToLower();
                }

                return fieldName == propertyName;
            });
        }
    }
}