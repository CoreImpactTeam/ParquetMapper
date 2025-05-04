using Parquet.Schema;

namespace CoreImpact.ParquetMapper.Exceptions
{
    public class ValueCannotBeNullException : Exception
    {
        public ValueCannotBeNullException(DataField field) : base($"Null is invalid value for non-nullable field \"{field.Name}\"") { }
    }
}
