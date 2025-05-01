using CoreImpact.ParquetMapper.Attributes;
using CoreImpact.ParquetMapper.Data;
using CoreImpact.ParquetMapper.Enums;
using CoreImpact.ParquetMapper.Exceptions;
using CoreImpact.ParquetMapper.Extensions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace CoreImpact.ParquetMapper.Test
{
    public abstract class BaseTest
    {
        protected ParquetSchema CreateParquetSchema(Type type)
        {
            var dataFields = new List<DataField>();

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                var colName = HandleAttributes(property, type, property.Name);

                if (string.IsNullOrEmpty(colName))
                {
                    continue;
                }

                dataFields.Add(new DataField(colName, property.PropertyType));
            }

            return new ParquetSchema(dataFields);
        }
        protected async Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new()
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
        public async Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int batchSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new()
        {
            var batch = new List<TDataType>(batchSize);
            var parquetSchema = CreateParquetSchema(typeof(TDataType));

            await using Stream stream = File.Create(path);
            using var writer = await ParquetWriter.CreateAsync(parquetSchema, stream, cancellationToken: cancellationToken);

            await foreach (var item in data.WithCancellation(cancellationToken))
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

        protected async Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : class, new()
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
        protected async IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, [EnumeratorCancellation] CancellationToken cancellationToken = default) where TDataType : new()
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
        protected string HandleAttributes(PropertyInfo property, Type type, string propertyName)
        {
            var attributes = property.GetCustomAttributes();
            var typeAttributes = type.GetCustomAttributes();

            if (attributes == null)
            {
                return propertyName;
            }

            if (attributes.Any(x => x.GetType() == typeof(IgnorePropertyAttribute)))
            {
                return null;
            }

            if (attributes.FirstOrDefault(x => x.GetType() == typeof(HasParquetColNameAttribute)) is HasParquetColNameAttribute colNameAttribute)
            {
                if (string.IsNullOrEmpty(colNameAttribute.ColName))
                {
                    throw new NullColNameException();
                }

                propertyName = colNameAttribute.ColName;
            }

            var ignoreCasingAttribute = typeAttributes.OfType<IgnoreCasingAttribute>().FirstOrDefault() ?? attributes.OfType<IgnoreCasingAttribute>().FirstOrDefault();

            if (ignoreCasingAttribute != null)
            {
                foreach (var flag in ignoreCasingAttribute.FilterFlags.GetActiveFlags())
                {
                    switch (flag)
                    {
                        case FilterFlags.Hyphen:
                            propertyName = propertyName.Replace("-", "");
                            break;

                        case FilterFlags.Underscore:
                            propertyName = propertyName.Replace("_", "");
                            break;

                        case FilterFlags.Space:
                            propertyName = propertyName.Replace(" ", "");
                            break;
                    }
                }

                propertyName = propertyName.ToLower();
            }

            return propertyName;
        }
        protected string ComputeHash(string fileName)
        {
            using (SHA256 sha = SHA256.Create())
            using (FileStream stream = File.OpenRead(fileName))
            {
                byte[] hashBytes = sha.ComputeHash(stream);

                return BitConverter.ToString(hashBytes).Replace("-", "");
            }
        }
        private async Task WriteBatch<T>(ParquetWriter writer, IEnumerable<T> batch, ParquetSchema schema)
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
    }
}
