using ParquetMapper.Test.Attributes.TestTypes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Test.Attributes.TestData
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
