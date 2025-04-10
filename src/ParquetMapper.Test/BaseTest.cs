using Parquet.Schema;
using ParquetMapper.Attributes;
using ParquetMapper.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit.Sdk;

namespace ParquetMapper.Test
{
    public abstract class BaseTest
    {
        protected ParquetSchema CreateParquetSchema(Type type)
        {
            var dataFields = new List<DataField>();

            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in properties)
            {
                var attributes = property.GetCustomAttributes();

                var colName = HandleAttributes(attributes, property.Name);

                if (string.IsNullOrEmpty(colName))
                {
                    continue;
                }

                dataFields.Add(new DataField(colName, property.PropertyType));
            }

            return new ParquetSchema(dataFields);
        }
        private string HandleAttributes(IEnumerable<Attribute>? attributes, string propertyName)
        {
            if (attributes == null)
            {
                return propertyName;
            }

            if (attributes.Any(x => x.GetType() == typeof(IgnorePropertyAttribute)))
            {
                return null;
            }

            foreach(var attribute in attributes)
            {
                switch (attribute)
                {
                    case HasParquetColNameAttribute temp:
                        if (string.IsNullOrEmpty(temp.ColName))
                        {
                            // TODO: create 'NullColNameException'
                            throw new NullColNameException();
                        }

                        propertyName = temp.ColName;
                        break;

                    // TODO
                }
            }

            return propertyName;
        }
    }
}
