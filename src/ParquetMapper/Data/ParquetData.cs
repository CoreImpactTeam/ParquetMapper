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
    /// <summary>
    /// Represents data read from a Parquet file, including the row groups, schema, metadata, and custom metadata.
    /// </summary>
    /// <typeparam name="TDataType">The type of the data objects within the Parquet file.</typeparam>
    public class ParquetData<TDataType>
    {
        private readonly TDataType[][] _data;
        private readonly int _rowGroupCount;
        private readonly ParquetSchema _schema;
        private readonly FileMetaData? _metadata;
        private readonly Dictionary<string, string>? _customMetadata;

        /// <summary>
        /// The number of row groups in the Parquet data.
        /// </summary>
        public int RowGroupCount => _rowGroupCount;

        /// <summary>
        /// The schema of the Parquet data.
        /// </summary>
        public ParquetSchema Schema => _schema;

        /// <summary>
        /// The metadata of the Parquet file.
        /// </summary>
        public FileMetaData? Metadata => _metadata;

        /// <summary>
        /// Custom metadata stored in the Parquet file.
        /// </summary>
        public Dictionary<string, string>? CustomMetadata => _customMetadata;

        /// <summary>
        /// Accesses a specific row group by its index.
        /// </summary>
        /// <param name="rowGroup">The index of the row group to retrieve.</param>
        /// <returns>An array of <typeparamref name="TDataType"/> representing the data in the specified row group.</returns>
        /// <exception cref="OutOfRowGroupRangeException">Thrown if the <paramref name="rowGroup"/> index is out of bounds.</exception>
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

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetData{TDataType}"/> class.
        /// </summary>
        /// <param name="data">The array of row groups, where each element is an array of <typeparamref name="TDataType"/>.</param>
        /// <param name="rowGroupCount">The number of row groups.</param>
        /// <param name="schema">The Parquet schema.</param>
        /// <param name="metadata">The Parquet file metadata.</param>
        /// <param name="customMetadata">Custom metadata.</param>
        public ParquetData(TDataType[][] data, int rowGroupCount, ParquetSchema schema, FileMetaData metadata, Dictionary<string, string> customMetadata)
        {
            _data = data;
            _rowGroupCount = rowGroupCount;
            _schema = schema;
            _metadata = metadata;
            _customMetadata = customMetadata;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ParquetData{TDataType}"/> class by reading data from a <see cref="ParquetReader"/>.
        /// </summary>
        /// <param name="data">The array of row groups, where each element is an array of <typeparamref name="TDataType"/>.</param>
        /// <param name="parquetReader">The <see cref="ParquetReader"/> from which the data and metadata were read.</param>
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
