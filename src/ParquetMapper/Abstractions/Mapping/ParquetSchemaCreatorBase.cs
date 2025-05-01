using CoreImpact.ParquetMapper.Abstractions.Interfaces;
using Parquet.Schema;

namespace CoreImpact.ParquetMapper.Abstractions.Mapping
{
    public abstract class ParquetSchemaCreatorBase : ISchemaCreator
    {
        public abstract ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        public abstract ParquetSchema CreateParquetSchema(Type type);
    }
}
