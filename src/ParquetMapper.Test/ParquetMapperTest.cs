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
        public void Test_Read_Parqut_File()
        {
            // TODO
        }
        [Fact]
        public void Test_Write_Parqut_File()
        {
            // TODO
        }
        [Fact]
        public void Throw_IncompatibleSchemaTypeException()
        {
            // TODO
        }
    }
}
