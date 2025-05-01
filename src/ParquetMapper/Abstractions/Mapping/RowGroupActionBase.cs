using CoreImpact.ParquetMapper.Attributes.Processing;
using CoreImpact.ParquetMapper.Exceptions;
using CoreImpact.ParquetMapper.Extensions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace CoreImpact.ParquetMapper.Abstractions.Mapping
{
    /// <summary>
    /// An abstract base class for writing row groups in the Parquet format.
    /// Provides caching of type metadata to enhance performance.
    /// </summary>
    public abstract class RowGroupActionBase : ParquetSchemaCreatorBase
    {
        /// <summary>
        /// A cache of type metadata, where the key is the <see cref="Type"/> of the object,
        /// and the value is the <see cref="TypeMetadata"/> for that type.
        /// The cache is static and thread-safe for sharing across all instances of the class.
        /// </summary>
        private static readonly ConcurrentDictionary<Type, TypeMetadata> _typeMetadataCache = new();

        /// <summary>
        /// A cache of attribute transformation contexts, where the key is the <see cref="Type"/> of the object,
        /// and the value is the <see cref="AttributeTransformContext"/> for that type.
        /// The cache is static and thread-safe for sharing across all instances of the class.
        /// </summary>
        private static readonly ConcurrentDictionary<Type, AttributeTransformContext> _typeTransformCache = new();

        /// <summary>
        /// Retrieves or creates an <see cref="AttributeTransformContext"/> for the specified type <typeparamref name="T"/>.
        /// If a context for this type already exists in the cache, it is returned; otherwise, a new one is created and cached.
        /// </summary>
        /// <typeparam name="T">The type for which to retrieve or create the transform context.</typeparam>
        /// <returns>An <see cref="AttributeTransformContext"/> instance for the specified type.</returns>
        protected AttributeTransformContext GetOrCreateTransformContext<T>()
        {
            Type type = typeof(T);

            return GetOrCreateTransformContext(type);
        }

        /// <summary>
        /// Retrieves or creates an <see cref="AttributeTransformContext"/> for the specified <see cref="Type"/>.
        /// If a context for this type already exists in the cache, it is returned; otherwise, a new one is created and cached.
        /// </summary>
        /// <param name="type">The type for which to retrieve or create the transform context.</param>
        /// <returns>An <see cref="AttributeTransformContext"/> instance for the specified type.</returns>
        protected AttributeTransformContext GetOrCreateTransformContext(Type type)
        {
            return _typeTransformCache.GetOrAdd(type, _ => new AttributeTransformContext(type));
        }

        /// <summary>
        /// Retrieves the metadata for the specified type <typeparamref name="T"/>.
        /// If the metadata for this type already exists in the cache, it is returned from the cache.
        /// Otherwise, new metadata is created using <see cref="CreateTypeMetadata(ParquetSchema, Type)"/>,
        /// added to the cache, and then returned.
        /// </summary>
        /// <typeparam name="T">The type of objects for which to retrieve metadata.</typeparam>
        /// <param name="schema">The Parquet schema that describes the data structure.</param>
        /// <returns>A <see cref="TypeMetadata"/> instance containing the metadata for the type <typeparamref name="T"/>.</returns>
        protected TypeMetadata GetOrCreateMetadata<T>(ParquetSchema? schema = null)
        {
            Type type = typeof(T);

            return GetOrCreateMetadata(type, schema);
        }

        /// <summary>
        /// Retrieves the metadata for the specified <see cref="Type"/>.
        /// If the metadata for this type already exists in the cache, it is returned from the cache.
        /// Otherwise, new metadata is created using <see cref="CreateTypeMetadata(ParquetSchema, Type)"/>,
        /// added to the cache, and then returned.
        /// </summary>
        /// <param name="schema">The Parquet schema that describes the data structure.</param>
        /// <param name="type">The type of objects for which to retrieve metadata.</param>
        /// <returns>A <see cref="TypeMetadata"/> instance containing the metadata for the specified <paramref name="type"/>.</returns>
        protected TypeMetadata GetOrCreateMetadata(Type type, ParquetSchema? schema = null)
        {
            return _typeMetadataCache.GetOrAdd(type, _ => CreateTypeMetadata(type, schema));
        }

        /// <summary>
        /// Creates the type metadata for a given <see cref="Type"/> to be written to Parquet.
        /// This includes obtaining property information, creating getters for fast access to values,
        /// and mapping property names to fields in the Parquet schema.
        /// </summary>
        /// <param name="schema">The Parquet schema that describes the data structure.</param>
        /// <param name="type">The type of objects for which to create metadata.</param>
        /// <returns>A <see cref="TypeMetadata"/> instance containing the created metadata.</returns>
        private TypeMetadata CreateTypeMetadata(Type type, ParquetSchema? schema = null)
        {
            schema ??= CreateParquetSchema(type);

            AttributeTransformContext transformContext = GetOrCreateTransformContext(type);

            PropertyInfo[] properties = transformContext.Properties as PropertyInfo[] ?? transformContext.Properties.ToArray();

            Dictionary<PropertyInfo, Func<object, object>> getters = properties.ToDictionary(
                property => property,
                property =>
                {
                    ParameterExpression parameter = Expression.Parameter(typeof(object), "instance");
                    Expression castInstance = Expression.Convert(parameter, type);
                    Expression propertyAccess = Expression.Property(castInstance, property);
                    Expression convertToObject = Expression.Convert(propertyAccess, typeof(object));
                    return Expression.Lambda<Func<object, object>>(convertToObject, parameter).Compile();
                });

            Dictionary<PropertyInfo, Action<object, object>> setters = properties.ToDictionary(
                 property => property,
                 property =>
                 {
                     ParameterExpression instanceParam = Expression.Parameter(typeof(object), "instance");
                     ParameterExpression valueParam = Expression.Parameter(typeof(object), "value");
                     Expression castInstance = Expression.Convert(instanceParam, type);
                     Expression castValue = Expression.Convert(valueParam, property.PropertyType);
                     Expression propertyAccess = Expression.Property(castInstance, property);
                     BinaryExpression assignExp = Expression.Assign(propertyAccess, castValue);
                     return Expression.Lambda<Action<object, object>>(assignExp, instanceParam, valueParam).Compile();
                 });

            if (!schema.TryCompareSchema(type, out var valuePairs))
            {
                throw new IncompatibleSchemaTypeException(schema, type);
            }

            var dataFields = valuePairs.ToDictionary(x => x.Value, x => schema.FindDataField(x.Key));

            return new TypeMetadata(properties, getters, setters, dataFields);
        }

        /// <summary>
        /// Asynchronously reads a row group and populates the provided buffer with column data.
        /// </summary>
        /// <typeparam name="TDataType">The type to map the data to.</typeparam>
        /// <param name="rowGroupReader">The reader for the Parquet row group.</param>
        /// <param name="metadata">Mapping info containing field names and setters.</param>
        /// <param name="buffer">A preallocated array that will be filled with data.</param>
        /// <returns>
        /// True if the data was successfully read and written; false if an error occurred.
        /// </returns>
        protected async Task<bool> ReadGroupAndWriteToBuffer<TDataType>(ParquetRowGroupReader rowGroupReader, TypeMetadata metadata, TDataType[] buffer)
        {
            try
            {
                int rowGroupLength = (int)rowGroupReader.RowCount;

                foreach (var comparedKeyValuePair in metadata.DataFields)
                {
                    var dataColumn = await rowGroupReader.ReadColumnAsync(comparedKeyValuePair.Value);

                    if (metadata.Setters.TryGetValue(comparedKeyValuePair.Key, out var setter))
                    {
                        for (int i = 0; i < dataColumn.Data.Length; i++)
                        {
                            setter(buffer[i], dataColumn.Data.GetValue(i));
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error in ReadGroupAndWriteToBuffer: {ex}");
                return false;
            }
        }

        /// <summary>
        /// Asynchronously writes a batch of objects of type <typeparamref name="T"/> as a row group
        /// to the specified <see cref="ParquetWriter"/>.
        /// Uses the provided <see cref="ParquetSchema"/> to determine the data structure.
        /// The metadata for the type <typeparamref name="T"/> is retrieved or created using
        /// <see cref="GetOrCreateMetadata{T}(ParquetSchema)"/>.
        /// </summary>
        /// <typeparam name="T">The type of objects in the batch.</typeparam>
        /// <param name="writer">The <see cref="ParquetWriter"/> instance to which the row group will be written.</param>
        /// <param name="batch">The collection of objects to write.</param>
        /// <param name="schema">The Parquet schema that describes the structure of the batch data.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous write operation.</returns>
        protected async Task WriteRowGroupAsync<T>(ParquetWriter writer, IEnumerable<T> batch, ParquetSchema schema)
        {
            T[] batchArray = batch as T[] ?? batch.ToArray();
            int rowCount = batchArray.Length;

            TypeMetadata metadata = GetOrCreateMetadata<T>(schema);

            using var rowGroup = writer.CreateRowGroup();

            var writeTasks = metadata.Properties.Zip(metadata.Getters, (property, getter) =>
            {
                Array columnData = Array.CreateInstance(property.PropertyType, rowCount);

                for (int i = 0; i < rowCount; i++)
                {
                    columnData.SetValue(getter.Value(batchArray[i]), i);
                }

                DataColumn dataColumn = new(metadata.DataFields[property], columnData);

                return rowGroup.WriteColumnAsync(dataColumn);
            });

            await Task.WhenAll(writeTasks);
        }

        /// <summary>
        /// An internal structure that holds metadata about a type, necessary for writing to Parquet.
        /// Includes information about properties, methods for getting their values (getters),
        /// and the mapping between property names and fields in the Parquet schema.
        /// </summary>
        /// <param name="Properties">An array of information about the type's properties.</param>
        /// <param name="Getters">An array of delegates (functions) that accept an object and return the value of the corresponding property.</param>
        /// <param name="DataFields">A dictionary that maps property names to their corresponding <see cref="DataField"/> in the Parquet schema.</param>
        protected readonly record struct TypeMetadata(
            PropertyInfo[] Properties,
            Dictionary<PropertyInfo, Func<object, object>> Getters,
            Dictionary<PropertyInfo, Action<object, object>> Setters,
            Dictionary<PropertyInfo, DataField> DataFields);
    }
}
