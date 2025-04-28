using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes
{
    /// <summary>
    /// Specifies a custom column name for a property when mapping to a Parquet file.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class HasParquetColNameAttribute : Attribute
    {
        public string ColName { get; }
        public HasParquetColNameAttribute(string name)
        {
            ColName = name;
        }
    }
}
