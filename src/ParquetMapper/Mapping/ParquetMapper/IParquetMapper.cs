using Parquet.Schema;
using ParquetMapper.Abstractions;
using ParquetMapper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ParquetMapper.Mapping.ParquetMapper
{
    public interface IParquetMapper : IParquetWriter, IParquetReader
    {
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
}
