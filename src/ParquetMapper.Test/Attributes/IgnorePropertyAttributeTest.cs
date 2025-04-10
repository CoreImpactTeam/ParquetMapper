using IgnoreImpact.ParquetMapper.Interfaces;
using Newtonsoft.Json.Linq;
using Parquet.Schema;
using ParquetMapper.Exceptions;
using ParquetMapper.Test;
using ParquetMapper.Test.Attributes.TestData;
using ParquetMapper.Test.Attributes.TestTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper.Test.Attributes
{
    public class IgnorePropertyAttributeTest : BaseTest
    {
        private readonly IParquetMapper _parquetMapper;
        public IgnorePropertyAttributeTest()
        {
            _parquetMapper = new ParquetMapper();
        }
        [Theory]
        [ClassData(typeof(IgnorePropertyTestData))]
        public void Test_Ignore_Property_Attribute(Type type)
        {
            var expected = CreateParquetSchema(type);

            var result = _parquetMapper.CreateParquetSchema(type);

            Assert.Equal(result, expected);
        }
        [Theory]
        [ClassData(typeof(NamePropertyTestData))]
        public void Test_Name_Property_Test_Ignore_Property_Attribute(Type type)
        {
            var expected = CreateParquetSchema(type);

            var result = _parquetMapper.CreateParquetSchema(type);

            Assert.Equal(result, expected);
        }
        [Fact]
        public void Throw_NullColNameException()
        {
            Assert.Throws<NullColNameException>(() => _parquetMapper.CreateParquetSchema(typeof(NullColNameTestType)));
        }
    }
}
