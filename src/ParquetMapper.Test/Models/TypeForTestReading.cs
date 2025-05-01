using CoreImpact.ParquetMapper.Attributes;

namespace CoreImpact.ParquetMapper.Test.Abstractions
{
    [IgnoreCasing]
    public class TypeForTestReading
    {
        public string TaskId { get; set; }
        public string Prompt { get; set; }
        public string CanonicalSolution { get; set; }
        public string Test { get; set; }
        public string EntryPoint { get; set; }
    }
}
