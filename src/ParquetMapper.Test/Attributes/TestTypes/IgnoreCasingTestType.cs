using CoreImpact.ParquetMapper.Attributes;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestTypes
{
    internal class IgnoreCasingTestType
    {
        [IgnoreCasing]
        internal class Type1
        {
            public int Property_1 { get; set; }
            public string Property_2 { get; set; }
            public string Property3 { get; set; }
        }
        internal class Type2
        {
            [IgnoreCasing]
            public int Property_1 { get; set; }
            public string Property2 { get; set; }
        }
    }
}
