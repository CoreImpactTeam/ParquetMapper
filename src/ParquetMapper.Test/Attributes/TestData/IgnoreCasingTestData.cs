﻿using CoreImpact.ParquetMapper.Test.Attributes.TestTypes;
using System.Collections;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestData
{
    public class IgnoreCasingTestData : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            yield return new object[] { typeof(IgnoreCasingTestType.Type1) };
            yield return new object[] { typeof(IgnoreCasingTestType.Type2) };
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
