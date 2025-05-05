using CoreImpact.ParquetMapper.Exceptions;
using CoreImpact.ParquetMapper.Extensions;
using CoreImpact.ParquetMapper.Mapping;
using CoreImpact.ParquetMapper.Test.Abstractions;
using CoreImpact.ParquetMapper.Test.Attributes.TestTypes;
using CoreImpact.ParquetMapper.Test.Models;
using Parquet.Schema;

namespace CoreImpact.ParquetMapper.Test
{
    public class ParquetMapperTest : BaseTest
    {
        public readonly IParquetMapper _parquetMapper;
        public ParquetMapperTest()
        {
            _parquetMapper = new Mapping.ParquetMapper();
        }
        [Fact]
        public void Test_Compare_Parquet_Schema()
        {
            var test = CreateParquetSchema(typeof(IgnorePropertyTestType.Type1));

            bool result = test.IsSchemaCompatible<IgnorePropertyTestType.Type1>();

            Assert.True(result);
        }
        [Fact]
        public async Task Test_Read_Parquet_File()
        {
            var test1 = await _parquetMapper.ReadParquetAsync<TypeForTestReading>("../../../data/test-00000-of-00001.parquet");
            var test = await ReadParquetAsync<TypeForTestReading>("../../../data/test-00000-of-00001.parquet");

            Assert.Equal(test1, test);
        }
        [Fact]
        public async Task Test_Write_Parquet_File()
        {
            List<TypeForTestWriting> data = new(1000000);

            for (int i = 0; i < data.Capacity; i++)
            {
                data.Add(new TypeForTestWriting
                {
                    Text = Guid.NewGuid().ToString(),
                    FloatValue = Random.Shared.NextDouble(),
                    Number = Random.Shared.Next()
                });
            }

            await _parquetMapper.WriteToParquetFileAsync(data, "test1.parquet", 2000);
            await WriteToParquetFileAsync(data, "test2.parquet", 2000);

            var expected = ComputeHash("test1.parquet");
            var result = ComputeHash("test2.parquet");

            Assert.Equal(expected, result);
        }
        [Fact]
        public async Task Test_Write_Parquet_File_With_Null()
        {
            var data = new List<TypeForWritingWithNull>{
                new TypeForWritingWithNull{ Count = null, Task = "text", Description = "text" },
                new TypeForWritingWithNull{ Count = 1, Task = "text" },
            };

            await _parquetMapper.WriteToParquetFileAsync(data, "test_with_null.parquet");
        }
        [Fact]
        public async Task Throw_ValueCannotBeNullException()
        {
            var data = new List<TypeForWritingWithNull>{
                new TypeForWritingWithNull{ Count = null, Task = null, Description = "text" },
                new TypeForWritingWithNull{ Count = 1, Task = "text" },
                new TypeForWritingWithNull{ }
            };

            await Assert.ThrowsAsync<ValueCannotBeNullException>(async () => await _parquetMapper.WriteToParquetFileAsync(data, "throw_valueCannotBeNullException.parquet"));
        }
        [Fact]
        public void Throw_IncompatibleSchemaTypeException()
        {
            var dataFields = new List<DataField>();

            dataFields.Add(new DataField(Guid.NewGuid().ToString(), typeof(int)));
            dataFields.Add(new DataField(Guid.NewGuid().ToString(), typeof(string)));

            var schema = new ParquetSchema(dataFields);

            Assert.Throws<IncompatibleSchemaTypeException>(() => schema.CompareSchema<IncompatibleSchemaTestType>());
        }
    }
}
