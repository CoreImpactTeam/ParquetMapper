using ParquetMapper.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Extensions
{
    /// <summary>
    /// Provides extension methods for the <see cref="FilterFlags"/> enumeration.
    /// </summary>
    public static class FilterFlagsExtensions
    {
        /// <summary>
        /// Gets a collection of the active flags within a <see cref="FilterFlags"/> enumeration value.
        /// </summary>
        /// <param name="filterEnum">The <see cref="FilterFlags"/> value to inspect.</param>
        /// <returns>An <see cref="IEnumerable{T}"/> of <see cref="FilterFlags"/> representing the active flags.</returns>
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
