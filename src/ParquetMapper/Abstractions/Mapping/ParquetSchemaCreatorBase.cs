using Parquet.Schema;
using ParquetMapper.Abstractions.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Abstractions.Mapping
{
    public abstract class ParquetSchemaCreatorBase : ISchemaCreator
    {
        public abstract ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        public abstract ParquetSchema CreateParquetSchema(Type type);
    }
}
