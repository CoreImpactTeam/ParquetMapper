using ParquetMapper.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Test.Attributes.TestTypes
{
    internal class NamePropertyTestType
    {
        internal class Type1
        {
            public int Property1 { get; set; }
            [HasParquetColName("Test Name")]
            public string Property2 { get; set; }
        }
        internal class Type2
        {
            public int Property1 { get; set; }
            [HasParquetColName(null)]
            public string Property2 { get; set; }
        }
    }
}
