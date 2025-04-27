using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Exceptions
{
    public class FailedToReadRowGroupException : Exception
    {
        public FailedToReadRowGroupException(int rowGroupIndex) : base($"Reading failed on index {rowGroupIndex}") { }
    }
}
