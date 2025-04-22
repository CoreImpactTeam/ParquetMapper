using Parquet.Schema;
using ParquetMapper.Attributes;
using ParquetMapper.Attributes.Processing.AttributeTransformContext;
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
        public static bool IsSchemaCompatible<TDataType>(this ParquetSchema parquetSchema) where TDataType : new()
        {
            var type = new TDataType().GetType();

            return parquetSchema.IsSchemaCompatible(type);
        }

        public static bool IsSchemaCompatible(this ParquetSchema parquetSchema, Type type)
        {
            try
            {
                CompareSchema(parquetSchema, type);
                return true;
            }
            catch (IncompatibleSchemaTypeException)
            {
                return false;
            }
        }

        // TODO 'TryCompareSchema(this ParquetSchema parquetSchema, Type type, out Dictionary<string, PropertyInfo> result)'
        public static Dictionary<string, PropertyInfo> CompareSchema<TDataType>(this ParquetSchema parquetSchema) where TDataType : new()
        {
            var type = new TDataType().GetType();

            return parquetSchema.CompareSchema(type);
        }
        public static Dictionary<string, PropertyInfo> CompareSchema(this ParquetSchema parquetSchema, Type type)
        {
            var attrTransformContext = new AttributeTransformContext(type);
            var nullabilityContext = new NullabilityInfoContext();
            var matchingFields = new Dictionary<string, PropertyInfo>();

            foreach (var prop in attrTransformContext.Properties)
            {
                var isNullableProp = nullabilityContext.Create(prop).WriteState == NullabilityState.Nullable;
                var isIgnorePropAttr = attrTransformContext.PropertyAttributesDict[prop].OfType<IgnorePropertyAttribute>().Any();

                if (isNullableProp || isIgnorePropAttr)
                {
                    continue;
                }

                var field = CompareWithAttributes(parquetSchema.DataFields, prop, attrTransformContext);

                if (field != null && field.ClrType == prop.PropertyType) // review the condition
                {
                    matchingFields.Add(field.Name, prop);
                }
            }

            if (matchingFields.Count != attrTransformContext.Properties.Count(prop => // review the condition
                    nullabilityContext.Create(prop).WriteState != NullabilityState.Nullable &&
                    prop.GetCustomAttribute<IgnorePropertyAttribute>() == null))
            {
                throw new IncompatibleSchemaTypeException(parquetSchema, type);
            }

            return matchingFields;
        }
        private static DataField? CompareWithAttributes(DataField[] dataFields, PropertyInfo property, IAttributeTransformContext transformContext)
        {

            return dataFields.FirstOrDefault(dataField =>
            {
                var fieldName = transformContext.TransformTextByPropertyAttributes(property, dataField.Name);
                var propertyName = transformContext.TransformTextByPropertyAttributes(property, property.Name); ;

                return fieldName == propertyName;
            });
        }
    }
}