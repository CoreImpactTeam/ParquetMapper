using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Abstractions.Interfaces
{
    public interface ISchemaCreator
    {
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
}
