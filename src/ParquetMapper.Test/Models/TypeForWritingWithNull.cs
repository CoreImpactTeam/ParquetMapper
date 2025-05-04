using CoreImpact.ParquetMapper.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoreImpact.ParquetMapper.Test.Models
{
    [IgnoreCasing]
    public class TypeForWritingWithNull
    {
        public int? Count { get; set; }
        public string Task { get; set; }
        public string? Description { get; set; }
    }
}
