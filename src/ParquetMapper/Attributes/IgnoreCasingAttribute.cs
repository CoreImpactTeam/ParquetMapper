using ParquetMapper.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes
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
