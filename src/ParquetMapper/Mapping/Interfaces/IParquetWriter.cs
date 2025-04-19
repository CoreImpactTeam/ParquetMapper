using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParquetMapper.Mapping.Interfaces
{
    public interface IParquetWriter
    {
        Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int rowGroupSize = 1024, CancellationToken cancellationToken = default) where TDataType : new();
        Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1024, CancellationToken cancellationToken = default) where TDataType : new();
    }
}
