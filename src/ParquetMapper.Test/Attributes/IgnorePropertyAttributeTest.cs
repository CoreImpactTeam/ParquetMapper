using CoreImpact.ParquetMapper.Exceptions;
using CoreImpact.ParquetMapper.Mapping;
using CoreImpact.ParquetMapper.Test.Attributes.TestData;
using CoreImpact.ParquetMapper.Test.Attributes.TestTypes;

namespace CoreImpact.ParquetMapper.Test.Attributes
{
    public class IgnorePropertyAttributeTest : BaseTest
    {
        private readonly IParquetMapper _parquetMapper;
        public IgnorePropertyAttributeTest()
        {
            _parquetMapper = new Mapping.ParquetMapper();
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
