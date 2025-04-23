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
    /// <summary>
    /// Contains extension methods for the <see cref="ParquetSchema"/> class that simplify integration
    /// with .NET types. These methods enable schema compatibility checks, comparisons, and provide mappings
    /// between Parquet field names and corresponding properties in data types.
    /// </summary>
    public static class ParquetSchemaExtensions
    {
        /// <summary>
        /// Checks whether the Parquet schema is compatible with the data type specified by the generic parameter <typeparamref name="TDataType"/>.
        /// </summary>
        /// <typeparam name="TDataType">The data type to check compatibility against. Must have a parameterless constructor.</typeparam>
        /// <param name="parquetSchema">The Parquet schema instance to verify.</param>
        /// <returns>
        /// <c>true</c> if the schema is compatible with <typeparamref name="TDataType"/>; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsSchemaCompatible<TDataType>(this ParquetSchema parquetSchema) where TDataType : new()
        {
            var type = new TDataType().GetType();
            return parquetSchema.IsSchemaCompatible(type);
        }

        /// <summary>
        /// Checks whether the Parquet schema is compatible with the specified <see cref="Type"/>.
        /// </summary>
        /// <param name="parquetSchema">The Parquet schema instance to verify.</param>
        /// <param name="type">The type against which to check the schema compatibility.</param>
        /// <returns>
        /// <c>true</c> if the schema is compatible with the specified type; otherwise, <c>false</c>.
        /// </returns>
        /// <remarks>
        /// This method determines compatibility by attempting to compare the schema with the given type. Any exceptions thrown during 
        /// comparison are caught, resulting in a return value of <c>false</c>.
        /// </remarks>
        public static bool IsSchemaCompatible(this ParquetSchema parquetSchema, Type type)
        {
            try
            {
                CompareSchema(parquetSchema, type);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Attempts to compare the Parquet schema with the data type specified by the generic parameter <typeparamref name="TDataType"/>
        /// and outputs a mapping between Parquet field names and corresponding properties from the type.
        /// </summary>
        /// <typeparam name="TDataType">The data type to compare against. Must have a parameterless constructor.</typeparam>
        /// <param name="parquetSchema">The Parquet schema to compare.</param>
        /// <param name="result">
        /// When the method returns, contains a <see cref="Dictionary{TKey, TValue}"/> where each key is a Parquet field name and each value is the
        /// corresponding <see cref="PropertyInfo"/> from the data type, if the schema is compatible; otherwise, <c>null</c>.
        /// </param>
        /// <returns>
        /// <c>true</c> if the schema comparison was successful and the schema is compatible with <typeparamref name="TDataType"/>; otherwise, <c>false</c>.
        /// </returns>
        public static bool TryCompareSchema<TDataType>(this ParquetSchema parquetSchema, out Dictionary<string, PropertyInfo>? result) where TDataType : new()
        {
            var type = new TDataType().GetType();
            return parquetSchema.TryCompareSchema(type, out result);
        }

        /// <summary>
        /// Attempts to compare the Parquet schema with the specified <see cref="Type"/> and outputs a mapping between Parquet field names
        /// and the corresponding properties from that type.
        /// </summary>
        /// <param name="parquetSchema">The Parquet schema to compare.</param>
        /// <param name="type">The type against which to compare the Parquet schema.</param>
        /// <param name="result">
        /// When the method returns, contains a <see cref="Dictionary{TKey, TValue}"/> mapping Parquet field names to <see cref="PropertyInfo"/> objects
        /// of the specified type if the schema comparison is successful; otherwise, <c>null</c>.
        /// </param>
        /// <returns>
        /// <c>true</c> if the schema comparison is successful and the schema is compatible with the specified type; otherwise, <c>false</c>.
        /// </returns>
        public static bool TryCompareSchema(this ParquetSchema parquetSchema, Type type, out Dictionary<string, PropertyInfo>? result)
        {
            try
            {
                result = CompareSchema(parquetSchema, type);
                return true;
            }
            catch
            {
                result = null;
                return false;
            }
        }

        /// <summary>
        /// Compares the Parquet schema with the data type specified by the generic parameter <typeparamref name="TDataType"/>
        /// and returns a mapping between matching Parquet field names and the corresponding properties of the type.
        /// </summary>
        /// <typeparam name="TDataType">The data type to compare against. Must have a parameterless constructor.</typeparam>
        /// <param name="parquetSchema">The Parquet schema to compare.</param>
        /// <returns>
        /// A <see cref="Dictionary{TKey, TValue}"/> where keys represent the Parquet field names and values represent the corresponding 
        /// <see cref="PropertyInfo"/> from the specified data type.
        /// </returns>
        /// <exception cref="IncompatibleSchemaTypeException">
        /// Thrown when the Parquet schema is deemed incompatible with the specified data type.
        /// </exception>
        public static Dictionary<string, PropertyInfo> CompareSchema<TDataType>(this ParquetSchema parquetSchema) where TDataType : new()
        {
            var type = new TDataType().GetType();
            return parquetSchema.CompareSchema(type);
        }

        /// <summary>
        /// Compares the Parquet schema with the specified <see cref="Type"/> and returns a mapping between matching Parquet field names
        /// and the corresponding properties from that type.
        /// </summary>
        /// <param name="parquetSchema">The Parquet schema to compare.</param>
        /// <param name="type">The type against which to compare the schema.</param>
        /// <returns>
        /// A <see cref="Dictionary{TKey, TValue}"/> where each key is a Parquet field name and each value is the corresponding <see cref="PropertyInfo"/>
        /// from the specified type.
        /// </returns>
        /// <exception cref="IncompatibleSchemaTypeException">
        /// Thrown when the schema is incompatible with the specified type, meaning not all required properties could be matched.
        /// </exception>
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

                if (field != null && field.ClrType == prop.PropertyType)
                {
                    matchingFields.Add(field.Name, prop);
                }
            }

            if (matchingFields.Count != attrTransformContext.Properties.Count(prop =>
                    nullabilityContext.Create(prop).WriteState != NullabilityState.Nullable &&
                    prop.GetCustomAttribute<IgnorePropertyAttribute>() == null))
            {
                throw new IncompatibleSchemaTypeException(parquetSchema, type);
            }

            return matchingFields;
        }
        /// <summary>
        /// Searches for the first <see cref="DataField"/> in the specified array whose transformed name,
        /// as computed from the property attributes, matches the transformed name of the provided <see cref="PropertyInfo"/>.
        /// </summary>
        /// <param name="dataFields">
        /// An array of <see cref="DataField"/> objects from the Parquet schema to be compared.
        /// </param>
        /// <param name="property">
        /// The property whose name will be transformed and compared against each data field's transformed name.
        /// </param>
        /// <param name="transformContext">
        /// The attribute transformation context that provides the transformation logic based on attributes 
        /// applied to the property.
        /// </param>
        /// <returns>
        /// The first matching <see cref="DataField"/> if one is found; otherwise, <c>null</c>.
        /// </returns>
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