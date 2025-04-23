using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Abstractions.Mapping
{
    /// <summary>
    /// An abstract base class for writing row groups in the Parquet format.
    /// Provides caching of type metadata to enhance performance.
    /// </summary>
    public abstract class RowGroupWriterBase
    {
        /// <summary>
        /// A cache of type metadata, where the key is the <see cref="Type"/> of the object,
        /// and the value is the <see cref="TypeMetadata"/> for that type.
        /// The cache is static and thread-safe for sharing across all instances of the class.
        /// </summary>
        private static readonly ConcurrentDictionary<Type, TypeMetadata> _typeMetadataCache = new();

        /// <summary>
        /// Retrieves the metadata for the specified type <typeparamref name="T"/>.
        /// If the metadata for this type already exists in the cache, it is returned from the cache.
        /// Otherwise, new metadata is created using <see cref="CreateTypeMetadata{T}(ParquetSchema)"/>,
        /// added to the cache, and then returned.
        /// </summary>
        /// <typeparam name="T">The type of objects for which to retrieve metadata.</typeparam>
        /// <param name="schema">The Parquet schema that describes the data structure.</param>
        /// <returns>A <see cref="TypeMetadata"/> instance containing the metadata for the type <typeparamref name="T"/>.</returns>
        protected TypeMetadata GetOrCreateMetadata<T>(ParquetSchema schema)
        {
            Type type = typeof(T);
            return _typeMetadataCache.GetOrAdd(type, _ => CreateTypeMetadata<T>(schema));
        }

        /// <summary>
        /// Creates the type metadata for <typeparamref name="T"/> to be written to Parquet.
        /// This includes obtaining property information, creating getters for fast access to values,
        /// and mapping property names to fields in the Parquet schema.
        /// </summary>
        /// <typeparam name="T">The type of objects for which to create metadata.</typeparam>
        /// <param name="schema">The Parquet schema that describes the data structure.</param>
        /// <returns>A <see cref="TypeMetadata"/> instance containing the created metadata.</returns>
        private TypeMetadata CreateTypeMetadata<T>(ParquetSchema schema)
        {
            PropertyInfo[] properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            Func<object, object>[] getters = properties.Select(property =>
            {
                ParameterExpression parameter = Expression.Parameter(typeof(object), "instance");
                Expression castInstance = Expression.Convert(parameter, typeof(T));
                Expression propertyAccess = Expression.Property(castInstance, property);
                Expression convertToObject = Expression.Convert(propertyAccess, typeof(object));
                return Expression.Lambda<Func<object, object>>(convertToObject, parameter).Compile();
            }).ToArray();

            Dictionary<string, DataField> dataFields = properties.ToDictionary(
                property => property.Name,
                property => schema.FindDataField(property.Name));

            return new TypeMetadata(properties, getters, dataFields);
        }

        /// <summary>
        /// Asynchronously writes a batch of objects as a row group to the specified <see cref="ParquetWriter"/>.
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
                    columnData.SetValue(getter(batchArray[i]), i);
                }

                DataColumn dataColumn = new(metadata.DataFields[property.Name], columnData);

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
            Func<object, object>[] Getters,
            Dictionary<string, DataField> DataFields);

    }
}
