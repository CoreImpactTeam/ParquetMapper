namespace CoreImpact.ParquetMapper.Attributes
{
    /// <summary>
    /// Marks a property to be ignored by the Parquet mapper; this property will not be written to or read from the Parquet file.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnorePropertyAttribute : Attribute
    {
        public bool IsPropertyIgnored { get; }
        public IgnorePropertyAttribute(bool isPropertyIgnored = true)
        {
            IsPropertyIgnored = isPropertyIgnored;
        }
    }
}
