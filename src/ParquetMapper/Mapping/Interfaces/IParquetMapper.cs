using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper.Interfaces
{
    public interface IParquetMapper
    {
        Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int batchSize = 1024, CancellationToken cancellationToken = default) where TDataType : new();
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
}
