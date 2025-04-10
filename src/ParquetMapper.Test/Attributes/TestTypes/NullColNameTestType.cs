using ParquetMapper.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Test.Attributes.TestTypes
{
    internal class NullColNameTestType
    {
        public int Property1 { get; set; }
        [HasParquetColName(null)]
        public int Property2 { get; set; }
    }
}
