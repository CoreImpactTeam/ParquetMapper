namespace CoreImpact.ParquetMapper.Abstractions.Interfaces
{
    public interface IParquetWriter
    {
        Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int rowGroupSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new();
        Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new();
    }
}
