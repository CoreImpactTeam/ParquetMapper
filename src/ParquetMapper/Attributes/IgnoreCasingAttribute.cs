using ParquetMapper.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes
{
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
