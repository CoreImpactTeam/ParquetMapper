using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class HasParquetColNameAttribute : Attribute
    {
        public string ColName { get; set; }
        public HasParquetColNameAttribute(string name)
        {
            ColName = name;
        }
    }
}
