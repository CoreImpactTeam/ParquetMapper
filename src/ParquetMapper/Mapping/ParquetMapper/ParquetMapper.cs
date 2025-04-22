using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetMapper.Abstractions;
using ParquetMapper.Attributes;
using ParquetMapper.Data;
using ParquetMapper.Exceptions;
using ParquetMapper.Extensions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParquetMapper.Mapping.ParquetMapper
{
    public class ParquetMapper : IParquetMapper, IParquetWriter, IParquetReader
    {
        public async Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int batchSize = 1024, CancellationToken cancellationToken = default) where TDataType : new()
        {
            var batch = new List<TDataType>(batchSize);
            var parquetSchema = CreateParquetSchema<TDataType>();
            using (Stream stream = File.Create(path))
            {
                using (ParquetWriter writer = await ParquetWriter.CreateAsync(parquetSchema, stream, cancellationToken: cancellationToken)) // must be 'await using'
                {
                    await foreach (var item in data)
                    {
                        batch.Add(item);
                        if (batch.Count == batchSize)
                        {
                            await WriteBatch(writer, batch, parquetSchema);
                            batch.Clear();
                        }
                    }
                    if (batch.Count > 0)
                    {
                        await WriteBatch(writer, batch, parquetSchema);
                    }
                }
            }
        }
        public ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new()
        {
            var type = typeof(TDataType);

            return CreateParquetSchema(type);
        }
        public ParquetSchema CreateParquetSchema(Type type)
        {
            var dataFields = new List<DataField>();

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                var temp = property.GetCustomAttributes();
                if (temp.Any(x => x.GetType() == typeof(IgnorePropertyAttribute)))
                {
                    continue;
                }

                var nameAttribute = property.GetCustomAttribute<HasParquetColNameAttribute>();

                if (nameAttribute is null)
                {
                    dataFields.Add(new DataField(property.Name, property.PropertyType));
                }
                else
                {
                    dataFields.Add(new DataField(nameAttribute.ColName, property.PropertyType));
                }
            }

            return new ParquetSchema(dataFields);
        }
        protected async Task WriteBatch<T>(ParquetWriter writer, IEnumerable<T> batch, ParquetSchema schema)
        {
            Type elementType = typeof(T);
            var properties = elementType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            T[] batchArray = batch.ToArray();

            using (var group = writer.CreateRowGroup())
            {
                foreach (var property in properties)
                {
                    Array data = Array.CreateInstance(property.PropertyType, batch.Count());
                    for (int i = 0; i < batchArray.Length; i++)
                    {
                        data.SetValue(property.GetValue(batchArray[i]), i);
                    }
                    DataColumn dataColumn = new DataColumn(schema.FindDataField(property.Name), data);
                    await group.WriteColumnAsync(dataColumn);
                }
            }
        }

        public Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1024, CancellationToken cancellationToken = default) where TDataType : new()
        {
            throw new NotImplementedException();
        }

        public Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, [EnumeratorCancellation] CancellationToken cancellationToken = default) where TDataType : new()
        {
            using (Stream fileStream = File.OpenRead(path))
            {
                using (var parquetReader = await ParquetReader.CreateAsync(fileStream))
                {

                    var dict = parquetReader.Schema.CompareSchema<TDataType>();

                    if (dict.Count == 0)
                    {
                        throw new IncompatibleSchemaTypeException(parquetReader.Schema, typeof(TDataType));
                    }

                    long rowsCount = parquetReader.Metadata.NumRows / parquetReader.RowGroupCount;
                    TDataType[] result = new TDataType[rowsCount];

                    for (int i = 0; i < rowsCount; i++)
                    {
                        result[i] = new();
                    }

                    for (int i = 0; i < parquetReader.RowGroupCount; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var rowGroup = await parquetReader.ReadEntireRowGroupAsync(i);

                        int rowGroupLength = rowGroup.First().Data.Length;

                        for (int row = 0; row < rowGroupLength; row++)
                        {
                            foreach (var column in rowGroup)
                            {
                                if (dict.TryGetValue(column.Field.Name, out var prop))
                                {
                                    prop.SetValue(result[row], column.Data.GetValue(row));
                                }
                            }
                        }

                        yield return result.Take(rowGroupLength).ToArray();
                    }
                }
            }
        }
    }
}
