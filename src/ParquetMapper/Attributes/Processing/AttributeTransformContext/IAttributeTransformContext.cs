using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace ParquetMapper.Attributes.Processing.AttributeTransformContext
{
    public interface IAttributeTransformContext
    {
        string TransformTextByPropertyAttributes(PropertyInfo prop, string targetText);
    }
}
