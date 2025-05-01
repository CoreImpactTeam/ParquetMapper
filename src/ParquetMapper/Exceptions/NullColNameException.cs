namespace CoreImpact.ParquetMapper.Exceptions
{
    public class NullColNameException : Exception
    {
        public NullColNameException() : base($"Col name can`t be null") { }
    }
}
