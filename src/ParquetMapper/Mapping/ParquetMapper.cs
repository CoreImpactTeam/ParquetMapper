using IgnoreImpact.ParquetMapper.Interfaces;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetMapper.Attributes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper
{
    public class ParquetMapper : IParquetMapper
    {
        public async Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int batchSize = 1024, CancellationToken cancellationToken = default) where TDataType : new()
        {
            var batch = new List<TDataType>(batchSize);
            var parquetSchema = CreateParquetSchema<TDataType>();
            using (Stream stream = File.Create(path))
            {
                using (ParquetWriter writer = await ParquetWriter.CreateAsync(parquetSchema, stream, cancellationToken: cancellationToken))
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
