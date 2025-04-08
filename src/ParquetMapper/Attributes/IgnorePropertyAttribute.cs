using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class IgnorePropertyAttribute : Attribute
    {
        public bool IsPropertyIgnored { get; set; }
        public IgnorePropertyAttribute(bool isPropertyIgnored = true)
        {
            IsPropertyIgnored = isPropertyIgnored;
        }
    }
}
