using Parquet.Schema;
using ParquetMapper.Data;
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
        Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int rowGroupSize = 1024, CancellationToken cancellationToken = default) where TDataType : new();
        Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1024, CancellationToken cancellationToken = default) where TDataType : new();
        Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default);
        IAsyncEnumerable<TDataType> ReadParquetAsAsyncEnumerable<TDataType>(string path, CancellationToken cancellationToken = default);
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
}
