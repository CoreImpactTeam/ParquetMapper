using System.Reflection;

namespace CoreImpact.ParquetMapper.Attributes.Processing
{
    public interface IAttributeTransformContext
    {
        string TransformTextByPropertyAttributes(PropertyInfo prop, string targetText);
    }
}
