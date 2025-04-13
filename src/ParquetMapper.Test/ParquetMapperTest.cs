using IgnoreImpact.ParquetMapper.Interfaces;
using Parquet.Schema;
using ParquetMapper.Attributes;
using ParquetMapper.Extensions;
using ParquetMapper.Test;
using ParquetMapper.Test.Attributes.TestTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper.Test
{
    public class ParquetMapperTest : BaseTest
    {
        public readonly IParquetMapper _parquetMapper;
        public ParquetMapperTest()
        {
            _parquetMapper = new ParquetMapper();
        }
        [Fact]
        public void Test_Compare_Parquet_Schema()
        {
            // TODO

            //var test = CreateParquetSchema(typeof(IgnorePropertyTestType.Type1));

            //bool result = test.CompareSchema<IgnorePropertyTestType.Type1>();

            //Assert.True(result);
        }
        [Fact]
        public async Task Test_Read_Parquet_File()
        {
            var test = await ReadParquetAsync<TestType>("../../../data/test-00000-of-00001.parquet");

            Assert.Equal(test, test);
        }
        [Fact]
        public void Test_Write_Parquet_File()
        {
            // TODO
        }
        [Fact]
        public void Throw_IncompatibleSchemaTypeException()
        {
            // TODO
        }
        [IgnoreCasing]
        internal class TestType
        {
            public string TaskId { get; set; } 
            public string prompt { get; set; }
            public string canonical_solution { get; set; }
            public string test { get; set; }
            public string entry_point { get; set; }
        }
    }
}
