using Parquet;
using Parquet.Schema;
using ParquetMapper.Attributes;
using ParquetMapper.Data;
using ParquetMapper.Exceptions;
using ParquetMapper.Extensions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit.Sdk;

namespace ParquetMapper.Test
{
    public abstract class BaseTest
    {
        protected ParquetSchema CreateParquetSchema(Type type)
        {
            var dataFields = new List<DataField>();

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                var attributes = property.GetCustomAttributes();

                var colName = HandleAttributes(attributes, property.Name);

                if (string.IsNullOrEmpty(colName))
                {
                    continue;
                }

                dataFields.Add(new DataField(colName, property.PropertyType));
            }

            return new ParquetSchema(dataFields);
        }
        protected async Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new()
        {
            // Открываем поток для чтения файла Parquet
            using (Stream fileStream = File.OpenRead(path))
            {
                using (var parquetReader = await ParquetReader.CreateAsync(fileStream))
                {
                    Stopwatch sw = new();

                    var dict = parquetReader.Schema.CompareSchema<TDataType>();

                    if (dict != null)
                    {
                        TDataType[][] result = new TDataType[parquetReader.RowGroupCount][];

                        for (int i = 0; i < parquetReader.RowGroupCount; i++)
                        {
                            var rowGroup = await parquetReader.ReadEntireRowGroupAsync(i);

                            long rowsCount = parquetReader.Metadata.NumRows / parquetReader.RowGroupCount;
                            result[i] = new TDataType[rowsCount];

                            for (int j = 0; j < rowsCount; j++)
                            {
                                result[i][j] = new();
                            }

                            foreach (var column in rowGroup)
                            {
                                dict.TryGetValue(column.Field.Name, out var prop);

                                if (prop == null)
                                {
                                    continue;
                                }

                                for (int rowIterator = 0; rowIterator < column.Data.Length; rowIterator++)
                                {
                                    prop.SetValue(result[i][rowIterator], column.Data.GetValue(rowIterator));
                                }
                            }
                        }

                        return new ParquetData<TDataType>(result, parquetReader);
                    }

                    throw new IncompatibleSchemaTypeException(parquetReader.Schema, typeof(TDataType));
                }

            }
        }
        private string HandleAttributes(IEnumerable<Attribute>? attributes, string propertyName)
        {
            if (attributes == null)
            {
                return propertyName;
            }

            if (attributes.Any(x => x.GetType() == typeof(IgnorePropertyAttribute)))
            {
                return null;
            }

            foreach(var attribute in attributes)
            {
                switch (attribute)
                {
                    case HasParquetColNameAttribute temp:
                        if (string.IsNullOrEmpty(temp.ColName))
                        {
                            // TODO: create 'NullColNameException'
                            throw new NullColNameException();
                        }

                        propertyName = temp.ColName;
                        break;

                    // TODO
                }
            }

            return propertyName;
        }
    }
}
