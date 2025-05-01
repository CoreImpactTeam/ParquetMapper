using CoreImpact.ParquetMapper.Abstractions.Interfaces;

namespace CoreImpact.ParquetMapper.Mapping
{
    public interface IParquetMapper : ISchemaCreator, IParquetWriter, IParquetReader
    {
    }
}
