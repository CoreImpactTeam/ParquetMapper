using Parquet.Schema;
using ParquetMapper.Attributes;
using ParquetMapper.Enums;
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
        public static bool CompareSchema(this ParquetSchema parquetSchema, Type type) // MUST RETURN Dictionary<DataField, PropertyInfo>
        {
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            var nullabilityContext = new NullabilityInfoContext();

            var ignoreCasingAttribute = type.GetCustomAttribute<IgnoreCasingAttribute>();

            bool isValid = properties.All(prop =>
            {
                var isNullableProp = nullabilityContext.Create(prop).WriteState == NullabilityState.Nullable;

                var isIgnorePropAttr = prop.GetCustomAttribute<IgnorePropertyAttribute>() != null ? true : false;

                if (isNullableProp | isIgnorePropAttr)
                {
                    return true;
                }

                var field = CompareWithAttributes(parquetSchema.DataFields, prop, ignoreCasingAttribute);

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
        private static DataField? CompareWithAttributes(DataField[] dataFields, PropertyInfo property, IgnoreCasingAttribute? ignoreCasingAttribute = null)
        {

            return dataFields.FirstOrDefault(dataField =>
            {
                var fieldName = dataField.Name;
                var propertyName = property.Name;

                if (property.GetCustomAttribute<HasParquetColNameAttribute>() is HasParquetColNameAttribute colNameAttribute)
                {
                    propertyName = colNameAttribute.ColName;
                }


                ignoreCasingAttribute ??= property.GetCustomAttribute<IgnoreCasingAttribute>();

                if (ignoreCasingAttribute != null)
                {
                    foreach (var flag in ignoreCasingAttribute.FilterFlags.GetActiveFlags())
                    {
                        switch (flag)
                        {
                            case FilterFlags.Hyphen:
                                fieldName = fieldName.Replace("-", "");
                                propertyName = propertyName.Replace("-", "");
                                break;

                            case FilterFlags.Underscore:
                                fieldName = fieldName.Replace("_", "");
                                propertyName = propertyName.Replace("_", "");
                                break;

                            case FilterFlags.Space:
                                fieldName = fieldName.Replace(" ", "");
                                propertyName = propertyName.Replace(" ", "");
                                break;
                        }
                    }
                    fieldName = fieldName.ToLower();
                    propertyName = propertyName.ToLower();
                }

                return fieldName == propertyName;
            });
        }
    }
}