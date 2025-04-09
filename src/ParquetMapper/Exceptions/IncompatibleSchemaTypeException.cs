using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Exceptions
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
