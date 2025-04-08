using IgnoreImpact.ParquetMapper.Interfaces;
using Parquet.Schema;
using ParquetMapper.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IgnoreImpact.ParquetMapper.Test
{
    public class ParquetMapperTest
    {
        public readonly IParquetMapper _parquetMapper;
        public ParquetMapperTest()
        {
            _parquetMapper = new ParquetMapper();
        }
    }
}
