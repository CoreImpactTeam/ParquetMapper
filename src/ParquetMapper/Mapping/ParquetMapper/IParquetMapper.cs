using Parquet.Schema;
using ParquetMapper.Abstractions.Interfaces;
using ParquetMapper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper.Mapping
{
    public interface IParquetMapper : ISchemaCreator, IParquetWriter, IParquetReader
    {
        IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
    }
}
