using CoreImpact.ParquetMapper.Attributes;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestTypes
{
    internal class NullColNameTestType
    {
        public int Property1 { get; set; }
        [HasParquetColName(null)]
        public int Property2 { get; set; }
    }
}
