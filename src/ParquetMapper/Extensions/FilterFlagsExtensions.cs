using ParquetMapper.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Extensions
{
    public static class FilterFlagsExtensions
    {
        public static IEnumerable<FilterFlags> GetActiveFlags(this FilterFlags filterEnum)
        {
            foreach (var flag in Enum.GetValues<FilterFlags>())
            {
                if (filterEnum.HasFlag(flag))
                {
                    yield return flag;
                }
            }
        }
    }
}
