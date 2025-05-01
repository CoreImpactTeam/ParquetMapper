using CoreImpact.ParquetMapper.Attributes.Processing;
using CoreImpact.ParquetMapper.Test.Attributes.TestData;
using System.Reflection;

namespace CoreImpact.ParquetMapper.Test.Attributes
{
    public class IgnoreCasingAttributeTest : BaseTest
    {
        [Theory]
        [ClassData(typeof(IgnoreCasingTestData))]
        public void Test_Ignore_Casing_Attribute(Type type)
        {
            List<string> expected = new();
            List<string> result = new();

            var attrTransContx = new AttributeTransformContext(type);

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                expected.Add(HandleAttributes(property, type, property.Name));
                result.Add(attrTransContx.TransformTextByPropertyAttributes(property, property.Name));
            }

            Assert.Equal(result, expected);
        }
    }
}
