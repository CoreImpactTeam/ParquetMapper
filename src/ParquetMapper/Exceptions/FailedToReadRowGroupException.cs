namespace CoreImpact.ParquetMapper.Exceptions
{
    public class FailedToReadRowGroupException : Exception
    {
        public FailedToReadRowGroupException(int rowGroupIndex) : base($"Reading failed on index {rowGroupIndex}") { }
    }
}
