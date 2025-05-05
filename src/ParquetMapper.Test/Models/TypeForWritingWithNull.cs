using CoreImpact.ParquetMapper.Attributes;

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
