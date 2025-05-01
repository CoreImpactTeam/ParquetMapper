using CoreImpact.ParquetMapper.Data;

namespace CoreImpact.ParquetMapper.Abstractions.Interfaces
{
    public interface IParquetReader
    {
        Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
        IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
    }
}
