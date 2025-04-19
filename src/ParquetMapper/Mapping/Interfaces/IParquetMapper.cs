using Parquet.Schema;
using ParquetMapper.Data;
using ParquetMapper.Mapping.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper.Interfaces
{
    public interface IParquetMapper : IParquetWriter, IParquetReader
    {
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
}
