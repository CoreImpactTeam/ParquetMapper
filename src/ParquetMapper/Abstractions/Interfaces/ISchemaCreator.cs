using Parquet.Schema;

namespace CoreImpact.ParquetMapper.Abstractions.Interfaces
{
    public interface ISchemaCreator
    {
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
}
