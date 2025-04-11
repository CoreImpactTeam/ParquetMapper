using Parquet;
using Parquet.Meta;
using Parquet.Schema;
using ParquetMapper.Exceptions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Data
{
    public class ParquetData<TDataType>
    {
        private readonly TDataType[][] _data;
        private readonly int _rowGroupCount;
        private readonly ParquetSchema _schema;
        private readonly FileMetaData? _metadata;
        private readonly Dictionary<string, string>? _customMetadata;

        public int RowGroupCount => _rowGroupCount;
        public ParquetSchema Schema => _schema;
        public FileMetaData? Metadata => _metadata;
        public Dictionary<string, string>? CustomMetadata => _customMetadata;
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

        public ParquetData(TDataType[][] data, int rowGroupCount, ParquetSchema schema, FileMetaData metadata, Dictionary<string, string> customMetadata)
        {
            _data = data;
            _rowGroupCount = rowGroupCount;
            _schema = schema;
            _metadata = metadata;
            _customMetadata = customMetadata;
        }
        public ParquetData(TDataType[][] data, ParquetReader parquetReader)
        {
            _data = data;
            _rowGroupCount = parquetReader.RowGroupCount;
            _schema = parquetReader.Schema;
            _metadata = parquetReader.Metadata;
            _customMetadata = parquetReader.CustomMetadata;
        }
    }
}
