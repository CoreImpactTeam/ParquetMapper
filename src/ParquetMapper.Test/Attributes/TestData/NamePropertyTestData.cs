using CoreImpact.ParquetMapper.Test.Attributes.TestTypes;
using System.Collections;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestData
{
    public class NamePropertyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { typeof(NamePropertyTestType.Type1) };
            yield return new object[] { typeof(NamePropertyTestType.Type2) };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
