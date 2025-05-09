﻿using CoreImpact.ParquetMapper.Enums;
using CoreImpact.ParquetMapper.Exceptions;
using CoreImpact.ParquetMapper.Extensions;
using System.Reflection;

namespace CoreImpact.ParquetMapper.Attributes.Processing
{
    /// <summary>
    /// Provides contextual information about a target type, including its properties and associated attributes.
    /// </summary>
    public class AttributeTransformContext : IAttributeTransformContext
    {
        public Type TargetType { get; init; }
        public IReadOnlyDictionary<PropertyInfo, IEnumerable<Attribute>> PropertyAttributesDict { get; }
        public IReadOnlyList<Attribute> TargetTypeAttributes { get; }
        public IReadOnlyList<PropertyInfo> Properties { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AttributeTransformContext"/> class for the specified target type.
        /// This constructor retrieves and caches the properties and attributes of the target type.
        /// </summary>
        /// <param name="targetType">The <see cref="Type"/> for which to create the transformation context.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="targetType"/> is null.</exception>
        /// <exception cref="ArgumentException">Thrown if <paramref name="targetType"/> is not a class.</exception>
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
        /// <returns>The transformed text as determined by the attributes applied, <see langword="null"/> if property have <see cref="IgnorePropertyAttribute"/></returns>
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

            if (PropertyAttributesDict[prop].OfType<IgnorePropertyAttribute>().FirstOrDefault() is IgnorePropertyAttribute)
            {
                return null;
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
