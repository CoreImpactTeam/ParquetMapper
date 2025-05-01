using CoreImpact.ParquetMapper.Data;

namespace CoreImpact.ParquetMapper.Exceptions
{
    public class OutOfRowGroupRangeException : Exception
    {
        public readonly ParquetData<object> parquetData;
        public OutOfRowGroupRangeException(object data) : base($"Invalid row group index for the {data}")
        {
            parquetData = (ParquetData<object>?)data;
        }
    }
}
