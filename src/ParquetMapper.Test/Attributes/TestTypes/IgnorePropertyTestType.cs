﻿using CoreImpact.ParquetMapper.Attributes;

namespace CoreImpact.ParquetMapper.Test.Attributes.TestTypes
{
    internal class IgnorePropertyTestType
    {
        internal class Type1
        {
            public int Property1 { get; set; }
            [IgnoreProperty]
            public string Property2 { get; set; }
            [IgnoreProperty]
            public string Property3 { get; set; }
        }
        internal class Type2
        {
            public int Property1 { get; set; }
            public string Property2 { get; set; }
        }
    }
}
