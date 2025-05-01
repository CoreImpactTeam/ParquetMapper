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
    public class ParquetData<TDataType> : IEquatable<ParquetData<TDataType>>
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

        public bool Equals(ParquetData<TDataType>? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (_rowGroupCount != other._rowGroupCount) return false;
            if (!EqualityComparer<ParquetSchema>.Default.Equals(_schema, other._schema)) return false;

            if (_metadata == null && other._metadata != null) return false;
            if (_metadata != null && other._metadata == null) return false;
            if (_metadata != null && other._metadata != null)
            {
                if (_metadata.RowGroups.Count != other._metadata.RowGroups.Count) return false;
                if (_metadata.CreatedBy != other._metadata.CreatedBy) return false;
                // Добавьте сравнение других значимых свойств FileMetaData
                // ...
                if (!CompareKeyValueMetadata(_metadata.KeyValueMetadata, other._metadata.KeyValueMetadata)) return false;
            }
            if (!DictionariesAreEqual(_customMetadata, other._customMetadata)) return false;

            if (_data.Length != other._data.Length) return false;
            for (int i = 0; i < _data.Length; i++)
            {
                if (!ArraysAreEqual(_data[i], other._data[i])) return false;
            }

            return true;
        }
        private bool CompareKeyValueMetadata(List<KeyValue>? d1, List<KeyValue>? d2)
        {
            if (ReferenceEquals(d1, d2)) return true;
            if (d1 is null || d2 is null) return false;
            if (d1.Count != d2.Count) return false;

            // Сортируем списки, чтобы порядок элементов не имел значения
            var sortedD1 = d1.OrderBy(kv => kv.Key).ToList();
            var sortedD2 = d2.OrderBy(kv => kv.Key).ToList();

            for (int i = 0; i < sortedD1.Count; i++)
            {
                if (sortedD1[i].Key != sortedD2[i].Key || sortedD1[i].Value != sortedD2[i].Value)
                {
                    return false;
                }
            }

            return true;
        }

        private bool ArraysAreEqual(TDataType[]? a1, TDataType[]? a2)
        {
            if (ReferenceEquals(a1, a2)) return true;
            if (a1 is null || a2 is null) return false;
            if (a1.Length != a2.Length) return false;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!AreEqual(a1[i], a2[i]))
                {
                    return false;
                }
            }
            return true;
        }
        private bool AreEqual<T>(T obj1, T obj2)
        {
            if (ReferenceEquals(obj1, obj2)) return true;
            if (obj1 is null || obj2 is null) return false;
            if (obj1.GetType() != obj2.GetType()) return false;

            var properties = typeof(T).GetProperties();
            foreach (var property in properties)
            {
                var value1 = property.GetValue(obj1);
                var value2 = property.GetValue(obj2);

                if (!Equals(value1, value2)) // Используем стандартный Equals для сравнения значений свойств
                {
                    return false;
                }
            }
            return true;
        }
        private bool DictionariesAreEqual(Dictionary<string, string>? d1, Dictionary<string, string>? d2)
        {
            if (ReferenceEquals(d1, d2)) return true;
            if (d1 is null || d2 is null) return false;
            if (d1.Count != d2.Count) return false;
            foreach (var pair in d1)
            {
                if (!d2.TryGetValue(pair.Key, out var value) || !EqualityComparer<string>.Default.Equals(pair.Value, value))
                {
                    return false;
                }
            }
            return true;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as ParquetData<TDataType>);
        }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(_rowGroupCount);
            hashCode.Add(_schema);
            hashCode.Add(_metadata);
            if (_customMetadata != null)
            {
                foreach (var pair in _customMetadata)
                {
                    hashCode.Add(pair.Key);
                    hashCode.Add(pair.Value);
                }
            }
            foreach (var rowGroup in _data)
            {
                foreach (var item in rowGroup)
                {
                    hashCode.Add(item);
                }
            }
            return hashCode.ToHashCode();
        }

        public static bool operator ==(ParquetData<TDataType>? left, ParquetData<TDataType>? right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ParquetData<TDataType>? left, ParquetData<TDataType>? right)
        {
            return !Equals(left, right);
        }
    }
}
