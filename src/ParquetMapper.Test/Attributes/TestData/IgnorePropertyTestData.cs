using CoreImpact.ParquetMapper.Test.Attributes.TestTypes;
using System.Collections;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestData
{

    public class IgnorePropertyTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { typeof(IgnorePropertyTestType.Type1) };
            yield return new object[] { typeof(IgnorePropertyTestType.Type2) };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
