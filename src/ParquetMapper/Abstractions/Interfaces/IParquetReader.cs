using ParquetMapper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParquetMapper.Abstractions.Interfaces
{
    public interface IParquetReader
    {
        Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
        IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
    }
}
