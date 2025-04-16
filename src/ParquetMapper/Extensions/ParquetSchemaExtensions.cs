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
        public static Dictionary<string, PropertyInfo> CompareSchema<TDataType>(this ParquetSchema parquetSchema) where TDataType : new()
        {
            var type = new TDataType().GetType();

            return parquetSchema.CompareSchema(type);
        }

        // вернуть старую версию `... bool CompareSchema(...)`

        public static Dictionary<string, PropertyInfo> CompareSchema(this ParquetSchema parquetSchema, Type type)
        {
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var nullabilityContext = new NullabilityInfoContext();
            var typeIgnoreCasingAttribute = type.GetCustomAttribute<IgnoreCasingAttribute>();
            var matchingFields = new Dictionary<string, PropertyInfo>();

            foreach (var prop in properties)
            {
                var isNullableProp = nullabilityContext.Create(prop).WriteState == NullabilityState.Nullable;
                var isIgnorePropAttr = prop.GetCustomAttribute<IgnorePropertyAttribute>() != null;

                if (isNullableProp || isIgnorePropAttr)
                {
                    continue;
                }

                var field = CompareWithAttributes(parquetSchema.DataFields, prop, typeIgnoreCasingAttribute);

                if (field != null && field.ClrType == prop.PropertyType) // review the condition
                {
                    matchingFields.Add(field.Name, prop);
                }
            }

            if (matchingFields.Count != properties.Count(prop => // review the condition
                    nullabilityContext.Create(prop).WriteState != NullabilityState.Nullable &&
                    prop.GetCustomAttribute<IgnorePropertyAttribute>() == null))
            {
                throw new IncompatibleSchemaTypeException(parquetSchema, type);
            }

            return matchingFields;
        }
        private static DataField? CompareWithAttributes(DataField[] dataFields, PropertyInfo property, IgnoreCasingAttribute? ignoreCasingAttribute = null)
        {

            return dataFields.FirstOrDefault(dataField =>
            {
                var fieldName = dataField.Name;
                var propertyName = property.Name;

                if (property.GetCustomAttribute<HasParquetColNameAttribute>() is HasParquetColNameAttribute colNameAttribute)
                {
                    propertyName = colNameAttribute.ColName ?? throw new NullColNameException();
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