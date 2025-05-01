using CoreImpact.ParquetMapper.Enums;

namespace CoreImpact.ParquetMapper.Attributes
{
    /// <summary>
    /// Instructs the mapper to ignore casing and specified separators (hyphen, underscore, space)
    /// when matching property names to Parquet column names. Can be applied to classes or properties.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property)]
    public class IgnoreCasingAttribute : Attribute
    {
        public FilterFlags FilterFlags { get; }
        public IgnoreCasingAttribute(FilterFlags filterFlags = FilterFlags.Hyphen | FilterFlags.Underscore | FilterFlags.Space)
        {
            FilterFlags = filterFlags;
        }
    }
}
