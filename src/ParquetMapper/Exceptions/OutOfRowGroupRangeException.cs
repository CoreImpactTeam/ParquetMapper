using ParquetMapper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Exceptions
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
