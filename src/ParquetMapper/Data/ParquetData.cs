using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Data
{
    public class ParquetData<TDataType>
    {
        private TDataType[][] _data;
        private uint _batchCount;
        public uint BatchCount => _batchCount;
        public IAsyncEnumerable<TDataType> DataStream { get; set; } 
        public TDataType[] this[int i]
        {
            get
            {
                // TODO
                return _data[i];
            }
        }
    }
}
