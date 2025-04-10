using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Exceptions
{
    public class NullColNameException : Exception
    {
        public NullColNameException() : base($"Col name can`t be null"){}
    }
}
