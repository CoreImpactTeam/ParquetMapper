using CoreImpact.ParquetMapper.Abstractions.Interfaces;
using CoreImpact.ParquetMapper.Abstractions.Mapping;
using CoreImpact.ParquetMapper.Data;
using CoreImpact.ParquetMapper.Exceptions;
using CoreImpact.ParquetMapper.Extensions;
using Parquet;
using Parquet.Schema;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace CoreImpact.ParquetMapper.Mapping
{
    /// <summary>
    /// Provides functionality for mapping .NET objects to and from Parquet files.
    /// Implements interfaces for both writing (<see cref="IParquetWriter"/>) and reading (<see cref="IParquetReader"/>) Parquet data,
    /// and inherits from <see cref="RowGroupActionBase"/> to leverage row group operations.
    /// </summary>
    public class ParquetMapper : RowGroupActionBase, IParquetMapper, IParquetWriter, IParquetReader
    {
        /// <summary>
        /// Asynchronously writes a collection of data objects to a Parquet file.
        /// </summary>
        /// <typeparam name="TDataType">The type of the data objects to write. Must have a parameterless constructor.</typeparam>
        /// <param name="data">The enumerable collection of data objects to write.</param>
        /// <param name="path">The full path to the Parquet file to create or overwrite.</param>
        /// <param name="rowGroupSize">The maximum number of rows to include in each Parquet row group. Defaults to 1024.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous write operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="data"/> or <paramref name="path"/> is null.</exception>
        /// <exception cref="IOException">Thrown if an error occurs while creating or writing to the file.</exception>
        public async Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new()
        {
            static async IAsyncEnumerable<TDataType> ToAsyncEnumerable(IEnumerable<TDataType> source)
            {
                foreach (var item in source)
                {
                    yield return item;
                }
            }

            await WriteToParquetFileAsync(ToAsyncEnumerable(data), path, rowGroupSize, cancellationToken);
        }

        /// <summary>
        /// Asynchronously writes an asynchronous stream of data objects to a Parquet file.
        /// </summary>
        /// <typeparam name="TDataType">The type of the data objects to write. Must have a parameterless constructor.</typeparam>
        /// <param name="data">The asynchronous enumerable stream of data objects to write.</param>
        /// <param name="path">The full path to the Parquet file to create or overwrite.</param>
        /// <param name="batchSize">The number of data objects to buffer before writing a row group to the Parquet file. Defaults to 1024.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous write operation.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="data"/> or <paramref name="path"/> is null.</exception>
        /// <exception cref="IOException">Thrown if an error occurs while creating or writing to the file.</exception>
        public async Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int batchSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new()
        {
            var batch = new List<TDataType>(batchSize);
            var parquetSchema = CreateParquetSchema<TDataType>();

            await using Stream stream = File.Create(path);
            using var writer = await ParquetWriter.CreateAsync(parquetSchema, stream, cancellationToken: cancellationToken);

            await foreach (var item in data.WithCancellation(cancellationToken))
            {
                batch.Add(item);
                if (batch.Count == batchSize)
                {
                    await WriteRowGroupAsync(writer, batch, parquetSchema);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await WriteRowGroupAsync(writer, batch, parquetSchema);
            }
        }

        /// <summary>
        /// Creates a Parquet schema based on the properties of the specified data type.
        /// </summary>
        /// <typeparam name="TDataType">The type whose properties will be used to define the Parquet schema.</typeparam>
        /// <returns>A new <see cref="ParquetSchema"/> representing the structure for the given type.</returns>
        public override ParquetSchema CreateParquetSchema<TDataType>()
        {
            var type = typeof(TDataType);
            return CreateParquetSchema(type);
        }

        /// <summary>
        /// Creates a Parquet schema based on the properties of the specified type.
        /// </summary>
        /// <param name="type">The <see cref="Type"/> whose properties will be used to define the Parquet schema.</param>
        /// <returns>A new <see cref="ParquetSchema"/> representing the structure for the given type.</returns>
        public override ParquetSchema CreateParquetSchema(Type type)
        {
            var dataFields = new List<DataField>();
            var transformContext = GetOrCreateTransformContext(type);
            var properties = transformContext.Properties;

            foreach (var property in properties)
            {
                var colName = transformContext.TransformTextByPropertyAttributes(property, property.Name);

                if (string.IsNullOrEmpty(colName))
                {
                    continue;
                }

                dataFields.Add(new DataField(colName, property.PropertyType));
            }

            return new ParquetSchema(dataFields);
        }

        /// <summary>
        /// Asynchronously reads all row groups from a Parquet file into a <see cref="ParquetData{TDataType}"/> object.
        /// </summary>
        /// <typeparam name="TDataType">The type of the data objects to read. Must have a parameterless constructor.</typeparam>
        /// <param name="path">The full path to the Parquet file to read from.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.</param>
        /// <returns>A <see cref="Task"/> that completes with a <see cref="ParquetData{TDataType}"/> object containing the read data and the Parquet reader.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="path"/> is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown if the Parquet file specified by <paramref name="path"/> does not exist.</exception>
        /// <exception cref="IOException">Thrown if an error occurs while reading from the file.</exception>
        /// <exception cref="IncompatibleSchemaTypeException">Thrown if the schema of the Parquet file is not compatible with <typeparamref name="TDataType"/>.</exception>
        public async Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new()
        {
            using var parquetReader = await ParquetReader.CreateAsync(path, cancellationToken: cancellationToken);

            TDataType[][] buffer = new TDataType[parquetReader.RowGroupCount][];
            var rowGroupIndex = 0;

            var data = ReadParquetAsAsyncEnumerable<TDataType>(path, cancellationToken);

            await foreach (var item in data)
            {
                buffer[rowGroupIndex++] = item;
            }

            return new ParquetData<TDataType>(buffer, parquetReader);
        }

        /// <summary>
        /// Asynchronously reads data from a Parquet file as an asynchronous enumerable of row groups.
        /// Each element in the enumerable is an array of <typeparamref name="TDataType"/> representing a single row group.
        /// </summary>
        /// <typeparam name="TDataType">The type of the data objects to read. Must have a parameterless constructor.</typeparam>
        /// <param name="path">The full path to the Parquet file to read from.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the operation to complete. Defaults to <see cref="CancellationToken.None"/>.</param>
        /// <returns>An <see cref="IAsyncEnumerable{T}"/> where each element is an array of <typeparamref name="TDataType"/> representing a row group.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="path"/> is null.</exception>
        /// <exception cref="FileNotFoundException">Thrown if the Parquet file specified by <paramref name="path"/> does not exist.</exception>
        /// <exception cref="IOException">Thrown if an error occurs while reading from the file.</exception>
        /// <exception cref="IncompatibleSchemaTypeException">Thrown if the schema of the Parquet file is not compatible with <typeparamref name="TDataType"/>.</exception>
        /// <exception cref="FailedToReadRowGroupException">Thrown if an error occurs while reading a specific row group.</exception>
        public async IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, [EnumeratorCancellation] CancellationToken cancellationToken = default) where TDataType : new()
        {
            await using Stream fileStream = File.OpenRead(path);
            using var parquetReader = await ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken);

            var typeMetadata = GetOrCreateMetadata<TDataType>(parquetReader.Schema);

            if (!parquetReader.Schema.IsSchemaCompatible<TDataType>())
            {
                throw new IncompatibleSchemaTypeException(parquetReader.Schema, typeof(TDataType));
            }

            long rowsCount = parquetReader.Metadata.RowGroups.Max(rg => rg.NumRows);

            TDataType[] buffer = ArrayPool<TDataType>.Shared.Rent((int)rowsCount);
            try
            {
                if (buffer[0] == null)
                {
                    for (int rowIndex = 0; rowIndex < rowsCount; rowIndex++)
                    {
                        buffer[rowIndex] = new TDataType();
                    }
                }

                for (int groupIndex = 0; groupIndex < parquetReader.RowGroupCount; groupIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(groupIndex);
                    int rowGroupLength = (int)rowGroupReader.RowCount;

                    bool writingSuccess = await ReadGroupAndWriteToBuffer(rowGroupReader, typeMetadata, buffer);
                    if (!writingSuccess)
                    {
                        throw new FailedToReadRowGroupException(groupIndex);
                    }

                    yield return buffer.Take(rowGroupLength).ToArray();
                }
            }
            finally
            {
                ArrayPool<TDataType>.Shared.Return(buffer);
            }
        }
    }
}
