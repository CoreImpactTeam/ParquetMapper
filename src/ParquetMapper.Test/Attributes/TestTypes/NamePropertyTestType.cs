using CoreImpact.ParquetMapper.Attributes;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestTypes
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
            [IgnoreProperty]
            public string Property2 { get; set; }
        }
    }
}
