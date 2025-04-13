using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Enums
{
    [Flags]
    public enum FilterFlags
    {
        Underscore = 1,
        Hyphen = 2,
        Space = 4
    }
}
