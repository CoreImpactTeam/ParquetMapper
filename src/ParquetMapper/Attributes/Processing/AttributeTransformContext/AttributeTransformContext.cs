using ParquetMapper.Enums;
using ParquetMapper.Exceptions;
using ParquetMapper.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes.Processing.AttributeTransformContext
{
    internal class AttributeTransformContext : IAttributeTransformContext
    {
        public Type TargetType { get; init; }
        public IReadOnlyDictionary<PropertyInfo, IEnumerable<Attribute>> PropertyAttributesDict { get; }
        public IReadOnlyList<Attribute> TargetTypeAttributes { get; }
        public IReadOnlyList<PropertyInfo> Properties { get; }
        public AttributeTransformContext(Type targetType)
        {
            if (targetType == null)
            {
                throw new ArgumentNullException(nameof(targetType));
            }

            if (!targetType.IsClass)
            {
                throw new ArgumentException();
            }

            TargetType = targetType;
            TargetTypeAttributes = targetType.GetCustomAttributes().ToList();
            Properties = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            PropertyAttributesDict = Properties.ToDictionary(prop => prop, prop => prop.GetCustomAttributes());
        }

        /// <summary>
        /// Transforms the target text based on the property and type-level attributes.
        /// If a <see cref="HasParquetColNameAttribute"/> is present on the property, its
        /// <c>ColName</c> value replaces the target text. Additionally, if an 
        /// <see cref="IgnoreCasingAttribute"/> is found either on the declaring type or 
        /// on the property, the method processes the text by removing specified characters 
        /// as indicated by active filter flags and converts 
        /// the final result to lower case.
        /// </summary>
        /// <param name="prop">The <see cref="PropertyInfo"/> representing the property to evaluate.</param>
        /// <param name="targetText">
        /// The input text to be transformed. If the property is decorated with a 
        /// <see cref="HasParquetColNameAttribute"/>, its <c>ColName</c> value will replace this text.
        /// </param>
        /// <returns>The transformed text as determined by the attributes applied.</returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the specified property is not found in the <c>PropertyAttributesDict</c>.
        /// </exception>
        /// <exception cref="NullColNameException">
        /// Thrown when the <see cref="HasParquetColNameAttribute"/> is present on the property 
        /// but its <c>ColName</c> value is null.
        /// </exception>
        public string TransformTextByPropertyAttributes(PropertyInfo prop, string targetText)
        {
            if (!PropertyAttributesDict.TryGetValue(prop, out var attributes))
            {
                throw new ArgumentException("Invalid property", nameof(prop));
            }

            var colNameAttr = attributes.OfType<HasParquetColNameAttribute>().FirstOrDefault();

            if (colNameAttr != null)
            {
                targetText = colNameAttr.ColName ?? throw new NullColNameException();
            }

            var ignoreCasingAttribute = TargetTypeAttributes.OfType<IgnoreCasingAttribute>().FirstOrDefault() ?? attributes.OfType<IgnoreCasingAttribute>().FirstOrDefault();

            if (ignoreCasingAttribute != null)
            {
                foreach (var flag in ignoreCasingAttribute.FilterFlags.GetActiveFlags())
                {
                    switch (flag)
                    {
                        case FilterFlags.Hyphen:
                            targetText = targetText.Replace("-", "");
                            break;

                        case FilterFlags.Underscore:
                            targetText = targetText.Replace("_", "");
                            break;

                        case FilterFlags.Space:
                            targetText = targetText.Replace(" ", "");
                            break;
                    }
                }

                targetText = targetText.ToLower();
            }

            return targetText;
        }
    }
}
