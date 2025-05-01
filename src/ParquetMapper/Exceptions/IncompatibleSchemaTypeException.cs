using Parquet.Schema;

namespace CoreImpact.ParquetMapper.Exceptions
{
    public class IncompatibleSchemaTypeException : Exception
    {
        public readonly ParquetSchema parquetSchema;
        public IncompatibleSchemaTypeException(ParquetSchema parquetSchema, Type type) : base($"Invalid {type} for {parquetSchema}")
        {
            this.parquetSchema = parquetSchema;
        }
    }
}
