using Parquet.Meta;
using Parquet.Schema;
using ParquetMapper.Exceptions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Data
{
    public class ParquetData<TDataType>
    {
        private TDataType[][] _data;
        private int _rowGroupCount;
        private ParquetSchema _schema;
        private FileMetaData _metadata;
        private Dictionary<string, string> _customMetadata;

        public int RowGroupCount => _rowGroupCount;
        public ParquetSchema Schema => _schema;
        public FileMetaData Metadata => _metadata;
        public Dictionary<string, string> CustomMetadata => _customMetadata;
        public TDataType[] this[int rowGroup]
        {
            get
            {
                if (rowGroup > _rowGroupCount)
                {
                    throw new OutOfRowGroupRangeException(this);
                }
                return _data[rowGroup];
            }
        }
    }
}
